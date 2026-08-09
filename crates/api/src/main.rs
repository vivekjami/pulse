//! Pulse API — REST + SSE fanout.
//!
//! ARCHITECTURE.md §5: browsers get one connection; frames are tagged by type.
//! A single Valkey consumer feeds a `tokio::sync::broadcast`, and each client
//! stream samples it — we drop frames rather than queue them, so a slow client
//! can never apply backpressure to the bus.

mod game;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{header, Method, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::{Json, Router};
use common::keys;
use futures_util::Stream;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

/// Live-wall frame cap (ARCHITECTURE.md §5: ~20/s, sample don't queue).
const MIN_FRAME_INTERVAL: Duration = Duration::from_millis(50);

/// Fanout buffer. Sized so a client that stalls briefly recovers by lagging
/// (dropping frames) rather than stalling the reader task.
const BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    edits: broadcast::Sender<String>,
    /// Confirmed bursts — a separate channel so a client that lags on the wall
    /// never drops a receipt, which is the one frame type worth delivering.
    confirmed: broadcast::Sender<String>,
    pub pool: Option<PgPool>,
    /// Connection for the game endpoints' short commands. Separate from the bus
    /// readers, which need a long response timeout for BLOCK.
    ///
    /// A ConnectionManager, not a MultiplexedConnection: the latter never
    /// reconnects, so a single dropped socket turned every game endpoint into a
    /// permanent 500 ("broken pipe") until the api was restarted by hand.
    pub redis: Option<redis::aio::ConnectionManager>,
}

#[tokio::main]
async fn main() -> Result<()> {
    common::config::init_tracing("api");

    let cache_wired = common::config::valkey_url().is_ok();
    let db_wired = common::config::database_url().is_ok();
    tracing::info!(db_wired, cache_wired, "cross-service env wiring");

    let (tx, _rx) = broadcast::channel::<String>(BROADCAST_CAPACITY);
    let (ctx, _crx) = broadcast::channel::<String>(BROADCAST_CAPACITY);

    // The receipts ledger is read-only here; the detector owns writes.
    let pool = match common::config::database_url() {
        Ok(url) => match common::db::connect(&url, 4).await {
            Ok(pool) => {
                // Idempotent — whichever of api/detector boots first wins.
                if let Err(err) = common::db::ensure_schema(&pool).await {
                    tracing::error!(error = %err, "schema ensure failed");
                }
                Some(pool)
            }
            Err(err) => {
                tracing::error!(error = %err, "postgres unavailable — /v1/events will 503");
                None
            }
        },
        Err(err) => {
            tracing::error!(%err, "no DATABASE_URL — /v1/events will 503");
            None
        }
    };

    // A short-command connection for the game endpoints. The bus readers build
    // their own with a long response timeout; mixing a BLOCK onto this one would
    // stall every queue read behind it.
    let redis = match common::config::valkey_url() {
        Ok(url) => match redis::Client::open(url) {
            Ok(client) => match redis::aio::ConnectionManager::new(client).await {
                Ok(con) => Some(con),
                Err(err) => {
                    tracing::error!(error = %err, "valkey unavailable for game endpoints");
                    None
                }
            },
            Err(err) => {
                tracing::error!(error = %err, "bad VALKEY_URL");
                None
            }
        },
        Err(_) => None,
    };

    let state = Arc::new(AppState {
        edits: tx.clone(),
        confirmed: ctx.clone(),
        pool,
        redis,
    });

    // One reader per stream for the whole process, regardless of client count.
    match common::config::valkey_url() {
        Ok(url) => {
            tokio::spawn(pump_stream(url.clone(), keys::BUS_EDITS, tx));
            tokio::spawn(pump_stream(url, keys::BUS_CONFIRMED, ctx));
        }
        Err(err) => tracing::error!(%err, "no VALKEY_URL — /v1/live will serve no frames"),
    }

    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/v1/live", get(live))
        .route("/v1/events", get(list_events))
        .route("/v1/events/{id}", get(get_event))
        // Phase 4 — conflict radar
        .route("/v1/controversy", get(game::controversy))
        .route("/v1/incidents", get(game::incidents))
        // Phase 3 — Vandal Patrol
        .route("/v1/players", axum::routing::post(game::create_player))
        .route("/v1/me", get(game::me))
        .route("/v1/patrol/queue", get(game::patrol_queue))
        .route("/v1/calls", axum::routing::post(game::create_call))
        .route("/v1/calls/{id}", get(game::get_call))
        .route("/v1/leaderboard", get(game::leaderboard))
        .route("/v1/flag", axum::routing::post(game::create_flag))
        // Phase 5 — Call the Surge
        .route("/v1/surge", axum::routing::post(game::create_bet))
        .route("/v1/watchlist", get(game::watchlist))
        .layer(cors_layer())
        .with_state(state);

    let port = common::config::port();
    // 0.0.0.0, never loopback — the L7 balancer routes to the VXLAN IP.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "api listening");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Tail `pulse:bus:edits` forever, forwarding payloads into the broadcast.
///
/// Starts at `$` (the live edge): a browser opening the wall wants what is
/// happening now, not a replay of the backlog the detector is still chewing.
async fn pump_stream(url: String, stream: &'static str, tx: broadcast::Sender<String>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match pump_stream_once(&url, stream, &tx).await {
            Ok(()) => tracing::warn!(stream, "bus reader ended — reconnecting"),
            // {:#} prints the whole anyhow chain; the outermost context alone
            // said only "XREAD from bus" and hid the real deserialization error.
            Err(err) => tracing::error!(stream, error = format!("{err:#}"), "bus reader failed"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// A blocking XREAD must outlive the client's own response timeout.
///
/// redis-rs defaults that timeout to 500ms, so `BLOCK 5000` was cancelled by the
/// client every single time no data arrived inside half a second — the reader
/// then treated it as a fatal error and reconnected. On the quiet `confirmed`
/// stream that meant a crash-loop that never delivered a frame; on `edits` it was
/// invisible because data almost always arrives first.
fn blocking_conn_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new().set_response_timeout(Some(Duration::from_secs(30)))
}

async fn pump_stream_once(
    url: &str,
    stream_key: &str,
    tx: &broadcast::Sender<String>,
) -> Result<()> {
    let client = redis::Client::open(url).context("opening valkey client")?;
    let mut con = client
        .get_multiplexed_async_connection_with_config(&blocking_conn_config())
        .await
        .context("connecting to valkey")?;
    tracing::info!(stream = stream_key, "bus reader connected");

    let mut cursor = "$".to_string();
    loop {
        // Option<_> is load-bearing: a BLOCK that expires with no new entries
        // replies nil, which does NOT deserialize into StreamReadReply. Reading
        // it as the concrete type made every timeout look like a fatal error, so
        // a quiet stream (`confirmed` sees a few frames an hour) crash-looped its
        // reader once every 5 seconds and never delivered anything.
        let reply: Option<redis::streams::StreamReadReply> = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(5_000)
            .arg("COUNT")
            .arg(500)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(&cursor)
            .query_async(&mut con)
            .await
            .context("XREAD from bus")?;

        let Some(reply) = reply else {
            continue; // block expired, nothing new — normal on a quiet stream
        };

        for stream in reply.keys {
            for mut entry in stream.ids {
                cursor = std::mem::take(&mut entry.id);
                if let Some(v) = entry.map.remove("payload") {
                    // from_redis_value keeps this stable across redis-rs
                    // representations of a bulk string.
                    match redis::from_redis_value::<String>(v) {
                        // Err only when there are no receivers; frames are
                        // meant to be dropped in that case.
                        Ok(payload) => {
                            let _ = tx.send(payload);
                        }
                        Err(err) => tracing::debug!(error = %err, "undecodable bus payload"),
                    }
                }
            }
        }
    }
}

/// Service banner.
async fn root() -> Json<Value> {
    Json(json!({
        "service": "pulse-api",
        "version": env!("CARGO_PKG_VERSION"),
        "phase": 5,
        "endpoints": [
            "/healthz", "/v1/live",
            "/v1/events", "/v1/events/:id",
            "/v1/controversy", "/v1/incidents",
            "/v1/players", "/v1/me", "/v1/patrol/queue",
            "/v1/calls", "/v1/calls/:id", "/v1/leaderboard", "/v1/flag",
            "/v1/surge", "/v1/watchlist"
        ],
        "source": "https://stream.wikimedia.org/v2/stream/recentchange",
    }))
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

/// `GET /v1/live` — multiplexed SSE. Phase 1 emits `edit` frames; `confirmed`,
/// `war` and `leaderboard` frames join on the same connection in later phases.
async fn live(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let edits = state.edits.subscribe();
    let confirmed = state.confirmed.subscribe();
    tracing::debug!("sse client attached");

    // Sampling lives in the per-client stream so one slow browser cannot slow
    // the bus reader or any other client. `confirmed` frames bypass the sampler
    // entirely — they arrive a few times an hour and each one is the product of
    // both gates, so dropping one to honour a frame-rate cap would be perverse.
    let stream = futures_util::stream::unfold(
        (edits, confirmed, Instant::now() - MIN_FRAME_INTERVAL),
        |(mut edits, mut confirmed, mut last)| async move {
            loop {
                tokio::select! {
                    biased;

                    // Receipts first: never starved by wall traffic.
                    res = confirmed.recv() => match res {
                        Ok(payload) => {
                            let event = Event::default().event("confirmed").data(payload);
                            return Some((Ok(event), (edits, confirmed, last)));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "client lagged on confirmed frames");
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },

                    res = edits.recv() => match res {
                        Ok(payload) => {
                            let now = Instant::now();
                            if now.duration_since(last) < MIN_FRAME_INTERVAL {
                                continue; // sample, don't queue
                            }
                            last = now;
                            let event = Event::default().event("edit").data(payload);
                            return Some((Ok(event), (edits, confirmed, last)));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::debug!(skipped, "sse client lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Query string for the receipts ledger.
#[derive(Debug, Deserialize)]
struct EventsQuery {
    limit: Option<i64>,
    kind: Option<String>,
}

/// `GET /v1/events?limit=&kind=` — the receipts ledger, newest first (§5).
async fn list_events(
    State(state): State<Arc<AppState>>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let pool = state.pool.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    // Clamped so a stray ?limit=1e9 cannot become a denial of service.
    let limit = q.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query(
        "SELECT id, article, kind, detected_at, peak_rate, distinct_eds, evidence
           FROM events
          WHERE ($1::text IS NULL OR kind = $1)
          ORDER BY detected_at DESC
          LIMIT $2",
    )
    .bind(q.kind.as_deref())
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "listing events");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let events: Vec<Value> = rows.iter().map(row_to_summary).collect();
    Ok(Json(json!({ "count": events.len(), "events": events })))
}

/// `GET /v1/events/:id` — the full evidence bundle (§5).
async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, StatusCode> {
    let pool = state.pool.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let row = sqlx::query(
        "SELECT id, article, kind, detected_at, gate1_at, gate2_at,
                peak_rate, distinct_eds, evidence, wikidata_qid
           FROM events WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "fetching event");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let mut out = row_to_summary(&row);
    out["gate1_at"] = json!(row
        .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("gate1_at")
        .ok()
        .flatten()
        .map(|t| t.to_rfc3339()));
    out["gate2_at"] = json!(row
        .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("gate2_at")
        .ok()
        .flatten()
        .map(|t| t.to_rfc3339()));
    out["wikidata_qid"] = json!(row
        .try_get::<Option<String>, _>("wikidata_qid")
        .ok()
        .flatten());
    Ok(Json(out))
}

fn row_to_summary(row: &sqlx::postgres::PgRow) -> Value {
    let detected_at: chrono::DateTime<chrono::Utc> = row.get("detected_at");
    json!({
        "id": row.get::<i64, _>("id"),
        "article": row.get::<String, _>("article"),
        "kind": row.get::<String, _>("kind"),
        // The permanent timestamp. Never updated — this is the proof.
        "detected_at": detected_at.to_rfc3339(),
        "peak_rate": row.try_get::<Option<f32>, _>("peak_rate").ok().flatten(),
        "distinct_eds": row.try_get::<Option<i32>, _>("distinct_eds").ok().flatten(),
        "evidence": row.get::<Value, _>("evidence"),
    })
}

/// The SPA is on its own origin and the game needs its signed cookie to travel,
/// so this cannot be `CorsLayer::permissive()`: browsers reject a wildcard
/// origin when credentials are included. Echo the request origin instead and
/// allow credentials explicitly.
///
/// Every list here is enumerated for the same reason the origin is — CORS
/// forbids pairing `Allow-Credentials: true` with a `*` in methods or headers,
/// and tower-http panics while building the layer rather than serve a config a
/// browser would reject. These are exactly what the SPA sends: GET/POST for the
/// endpoints, OPTIONS for the preflight, and Content-Type because `apiFetch`
/// posts JSON.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::mirror_request())
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE])
        .allow_credentials(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// tower-http validates the credentials/wildcard combination when the layer
    /// is BUILT, so an invalid pairing is a startup panic, not a bad response —
    /// it cost a full deploy cycle once. Constructing it here is the whole test.
    #[test]
    fn the_cors_layer_is_a_configuration_a_browser_would_accept() {
        let _ = cors_layer();
    }
}
