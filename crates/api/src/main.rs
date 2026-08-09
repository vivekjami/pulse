//! Pulse API — REST + SSE fanout.
//!
//! ARCHITECTURE.md §5: browsers get one connection; frames are tagged by type.
//! A single Valkey consumer feeds a `tokio::sync::broadcast`, and each client
//! stream samples it — we drop frames rather than queue them, so a slow client
//! can never apply backpressure to the bus.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::{Json, Router};
use common::keys;
use futures_util::Stream;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

/// Live-wall frame cap (ARCHITECTURE.md §5: ~20/s, sample don't queue).
const MIN_FRAME_INTERVAL: Duration = Duration::from_millis(50);

/// Fanout buffer. Sized so a client that stalls briefly recovers by lagging
/// (dropping frames) rather than stalling the reader task.
const BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone)]
struct AppState {
    edits: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    common::config::init_tracing("api");

    let cache_wired = common::config::valkey_url().is_ok();
    let db_wired = common::config::database_url().is_ok();
    tracing::info!(db_wired, cache_wired, "cross-service env wiring");

    let (tx, _rx) = broadcast::channel::<String>(BROADCAST_CAPACITY);
    let state = Arc::new(AppState { edits: tx.clone() });

    // One reader for the whole process, regardless of client count.
    match common::config::valkey_url() {
        Ok(url) => {
            tokio::spawn(pump_bus(url, tx));
        }
        Err(err) => tracing::error!(%err, "no VALKEY_URL — /v1/live will serve no frames"),
    }

    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/v1/live", get(live))
        // The wall is public, read-only data; the SPA is on its own origin.
        .layer(CorsLayer::permissive())
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
async fn pump_bus(url: String, tx: broadcast::Sender<String>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match pump_bus_once(&url, &tx).await {
            Ok(()) => tracing::warn!("bus reader ended — reconnecting"),
            Err(err) => tracing::error!(error = %err, "bus reader failed"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn pump_bus_once(url: &str, tx: &broadcast::Sender<String>) -> Result<()> {
    let client = redis::Client::open(url).context("opening valkey client")?;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .context("connecting to valkey")?;
    tracing::info!("bus reader connected");

    let mut cursor = "$".to_string();
    loop {
        let reply: redis::streams::StreamReadReply = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(5_000)
            .arg("COUNT")
            .arg(500)
            .arg("STREAMS")
            .arg(keys::BUS_EDITS)
            .arg(&cursor)
            .query_async(&mut con)
            .await
            .context("XREAD from bus")?;

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
        "phase": 1,
        "endpoints": ["/healthz", "/v1/live"],
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
    let rx = state.edits.subscribe();
    tracing::debug!("sse client attached");

    // Sampling lives in the per-client stream so one slow browser cannot slow
    // the bus reader or any other client.
    let stream = futures_util::stream::unfold(
        (rx, Instant::now() - MIN_FRAME_INTERVAL),
        |(mut rx, mut last)| async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        let now = Instant::now();
                        if now.duration_since(last) < MIN_FRAME_INTERVAL {
                            continue; // sample, don't queue
                        }
                        last = now;
                        let event = Event::default().event("edit").data(payload);
                        return Some((Ok(event), (rx, last)));
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(skipped, "sse client lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}
