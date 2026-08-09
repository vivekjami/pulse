//! The game surface — ARCHITECTURE.md §5, PLAN.md Phases 3 and 5.
//!
//! No auth beyond handle + signed cookie (§9: "no auth beyond handle+cookie
//! (hackathon)"). The cookie is an HMAC over the player id, so a client can hold
//! an identity without the server holding a session table, and cannot forge one
//! without the secret.

use std::io::Read;
use std::sync::{Arc, OnceLock};

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::Row;

use crate::AppState;

/// Vandal Patrol deadline (PLAN.md Phase 3).
const CALL_DEADLINE_MINS: i64 = 10;
/// Surge horizon (§3.2 `expires_at = placed_at + 60 min`).
const SURGE_WINDOW_MINS: i64 = 60;
/// Stake bounds — a hackathon ledger, not a casino.
const STAKE_MIN: i32 = 1;
const STAKE_MAX: i32 = 100;
/// Handles are display strings; keep them short and printable.
const HANDLE_MAX: usize = 24;

type HmacSha256 = Hmac<Sha256>;

/// Resolved once: the env var if set, else 32 fresh bytes from the OS.
static SECRET: OnceLock<Vec<u8>> = OnceLock::new();

/// The HMAC key for session cookies.
///
/// A *literal* fallback would be a publicly known key — with the signing key in
/// the repo, anyone could mint a cookie for player id 1 and take over the top of
/// the leaderboard. So an unset `PULSE_SESSION_SECRET` falls back to random
/// bytes instead: cookies then stop surviving a restart, which is the safe
/// failure for a game ledger, and never a forgeable one.
fn secret() -> &'static [u8] {
    SECRET.get_or_init(|| {
        if let Ok(from_env) = std::env::var("PULSE_SESSION_SECRET") {
            if !from_env.is_empty() {
                return from_env.into_bytes();
            }
        }
        // read_exact, not fs::read: /dev/urandom is an endless character device
        // with no EOF, so a read-to-end allocates until the process is killed.
        let mut key = [0u8; 32];
        match std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut key)) {
            Ok(()) => {
                tracing::warn!(
                    "PULSE_SESSION_SECRET unset — signing cookies with an ephemeral key; \
                     players will be logged out on restart"
                );
                key.to_vec()
            }
            Err(err) => panic!(
                "PULSE_SESSION_SECRET is unset and /dev/urandom is unreadable ({err}) — \
                 refusing to sign cookies with a guessable key"
            ),
        }
    })
}

fn sign(player_id: i64) -> String {
    let mut mac = HmacSha256::new_from_slice(secret()).expect("hmac key");
    mac.update(player_id.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time compare so a forged cookie can't be brute-forced byte by byte.
fn verify(player_id: i64, sig: &str) -> bool {
    let expected = sign(player_id);
    if expected.len() != sig.len() {
        return false;
    }
    expected
        .bytes()
        .zip(sig.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Read the player id out of the signed cookie, if present and valid.
pub fn player_from_cookies(headers: &HeaderMap) -> Option<i64> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let part = part.trim();
        let Some(value) = part.strip_prefix("pulse_player=") else {
            continue;
        };
        let (id_str, sig) = value.split_once('.')?;
        let id: i64 = id_str.parse().ok()?;
        if verify(id, sig) {
            return Some(id);
        }
    }
    None
}

fn require_player(headers: &HeaderMap) -> Result<i64, StatusCode> {
    player_from_cookies(headers).ok_or(StatusCode::UNAUTHORIZED)
}

fn pool(state: &AppState) -> Result<&sqlx::PgPool, StatusCode> {
    state.pool.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)
}

// ── POST /v1/players ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NewPlayer {
    pub handle: String,
}

/// Create (or re-attach to) a handle and return a signed cookie.
///
/// Re-attaching rather than rejecting a taken handle is deliberate: there are no
/// passwords, so "the handle is the identity" and a returning player just gets
/// their cookie back.
pub async fn create_player(
    State(state): State<Arc<AppState>>,
    Json(body): Json<NewPlayer>,
) -> Result<Response, StatusCode> {
    let pool = pool(&state)?;
    let handle = body.handle.trim();
    if handle.is_empty() || handle.chars().count() > HANDLE_MAX {
        return Err(StatusCode::BAD_REQUEST);
    }

    let row = sqlx::query(
        "INSERT INTO players (handle) VALUES ($1)
         ON CONFLICT (handle) DO UPDATE SET handle = EXCLUDED.handle
         RETURNING id, handle, elo, points",
    )
    .bind(handle)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "creating player");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let id: i64 = row.get("id");
    let cookie = format!(
        "pulse_player={id}.{sig}; Path=/; Max-Age=2592000; SameSite=None; Secure; HttpOnly",
        sig = sign(id)
    );

    let body = json!({
        "id": id,
        "handle": row.get::<String, _>("handle"),
        "elo": row.get::<f32, _>("elo"),
        "points": row.get::<i64, _>("points"),
    });
    Ok(([(header::SET_COOKIE, cookie)], Json(body)).into_response())
}

/// `GET /v1/me` — who the cookie says I am.
pub async fn me(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let id = require_player(&headers)?;
    let row = sqlx::query("SELECT id, handle, elo, points FROM players WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(json!({
        "id": row.get::<i64, _>("id"),
        "handle": row.get::<String, _>("handle"),
        "elo": row.get::<f32, _>("elo"),
        "points": row.get::<i64, _>("points"),
    })))
}

// ── GET /v1/patrol/queue ───────────────────────────────────────────────────

/// Candidates for Vandal Patrol, produced by the detector's eligibility filter.
pub async fn patrol_queue(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Value>, StatusCode> {
    let mut con = state
        .redis
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let limit = q.limit.unwrap_or(20).clamp(1, 60);

    let raw: Vec<String> = redis::cmd("LRANGE")
        .arg("pulse:patrol:queue")
        .arg(0)
        .arg(limit - 1)
        .query_async(&mut con)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "reading patrol queue");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items: Vec<Value> = raw
        .iter()
        .filter_map(|s| serde_json::from_str(s).ok())
        .collect();
    Ok(Json(json!({ "count": items.len(), "candidates": items })))
}

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    pub limit: Option<i64>,
}

// ── POST /v1/calls ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NewCall {
    pub article: String,
    pub rev_id: i64,
    /// true = "vandalism".
    pub verdict: bool,
}

/// Record a Vandal Patrol call and queue it for settlement.
pub async fn create_call(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<NewCall>,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let player_id = require_player(&headers)?;
    if body.article.is_empty() || body.rev_id <= 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let called_at = chrono::Utc::now();
    let deadline = called_at + chrono::Duration::minutes(CALL_DEADLINE_MINS);

    let row = sqlx::query(
        "INSERT INTO calls (player_id, article, rev_id, verdict, called_at, deadline)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(player_id)
    .bind(&body.article)
    .bind(body.rev_id)
    .bind(body.verdict)
    .bind(called_at)
    .bind(deadline)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "inserting call");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let call_id: i64 = row.get("id");

    // The detector polls this ZSET; score is the deadline.
    if let Some(mut con) = state.redis.clone() {
        let _: Result<i64, redis::RedisError> = redis::cmd("ZADD")
            .arg(common::keys::SETTLE_QUEUE)
            .arg(deadline.timestamp_millis())
            .arg(call_id)
            .query_async(&mut con)
            .await;
    }

    Ok(Json(json!({
        "id": call_id,
        "article": body.article,
        "rev_id": body.rev_id,
        "verdict": body.verdict,
        "deadline": deadline.to_rfc3339(),
        "settles_in_secs": CALL_DEADLINE_MINS * 60,
    })))
}

/// `GET /v1/calls/:id` — poll a call for its settled outcome.
pub async fn get_call(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let row = sqlx::query(
        "SELECT c.id, c.article, c.rev_id, c.verdict, c.called_at, c.deadline,
                c.outcome, c.settled_at, p.handle, p.elo
           FROM calls c JOIN players p ON p.id = c.player_id
          WHERE c.id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let outcome: Option<bool> = row.try_get("outcome").ok().flatten();
    let verdict: bool = row.get("verdict");
    Ok(Json(json!({
        "id": row.get::<i64, _>("id"),
        "article": row.get::<String, _>("article"),
        "verdict": verdict,
        "outcome": outcome,
        "correct": outcome.map(|o| o == verdict),
        "settled": outcome.is_some(),
        "deadline": row.get::<chrono::DateTime<chrono::Utc>, _>("deadline").to_rfc3339(),
        "handle": row.get::<String, _>("handle"),
        "elo": row.get::<f32, _>("elo"),
    })))
}

// ── POST /v1/surge ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NewBet {
    pub article: String,
    pub stake: i32,
}

/// Stake points that an article confirm-bursts within the hour.
pub async fn create_bet(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<NewBet>,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let player_id = require_player(&headers)?;
    if body.article.is_empty() || !(STAKE_MIN..=STAKE_MAX).contains(&body.stake) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let placed_at = chrono::Utc::now();
    let expires_at = placed_at + chrono::Duration::minutes(SURGE_WINDOW_MINS);

    let mut tx = pool.begin().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Deduct the stake now; a win returns it doubled. Guarded so a player
    // cannot go negative by racing two requests.
    let deducted = sqlx::query("UPDATE players SET points = points - $1 WHERE id = $2 AND points >= $1")
        .bind(i64::from(body.stake))
        .bind(player_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if deducted.rows_affected() == 0 {
        return Err(StatusCode::PAYMENT_REQUIRED);
    }

    let row = sqlx::query(
        "INSERT INTO surge_bets (player_id, article, stake, placed_at, expires_at)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(player_id)
    .bind(&body.article)
    .bind(body.stake)
    .bind(placed_at)
    .bind(expires_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "placing bet");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    tx.commit().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "id": row.get::<i64, _>("id"),
        "article": body.article,
        "stake": body.stake,
        "expires_at": expires_at.to_rfc3339(),
    })))
}

// ── POST /v1/flag ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NewFlag {
    pub article: String,
}

/// First Responder: flag an article pre-confirmation. If the detector confirms
/// it, the player's id lands on the receipt itself (§3.2 `first_flagger`).
///
/// `SET NX` so the FIRST flagger wins, not the last.
pub async fn create_flag(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<NewFlag>,
) -> Result<Json<Value>, StatusCode> {
    let player_id = require_player(&headers)?;
    if body.article.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut con = state.redis.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let claimed: Option<String> = redis::cmd("SET")
        .arg(format!("pulse:flag:{}", body.article))
        .arg(player_id)
        .arg("NX")
        .arg("EX")
        .arg(1_800)
        .query_async(&mut con)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "article": body.article,
        "first": claimed.is_some(),
        "window_secs": 1_800,
    })))
}

// ── GET /v1/leaderboard ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BoardQuery {
    /// `patrol` (ELO) or `surge` (points). Defaults to patrol.
    pub mode: Option<String>,
    pub limit: Option<i64>,
}

pub async fn leaderboard(
    State(state): State<Arc<AppState>>,
    Query(q): Query<BoardQuery>,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let mode = q.mode.as_deref().unwrap_or("patrol");

    // Two fixed queries rather than string-built ORDER BY — no interpolation
    // into SQL, so a crafted `mode` cannot reach the planner.
    let rows = match mode {
        "surge" => {
            sqlx::query(
                "SELECT handle, elo, points,
                        (SELECT count(*) FROM calls c WHERE c.player_id = p.id AND c.settled_at IS NOT NULL) AS settled
                   FROM players p ORDER BY points DESC, handle ASC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await
        }
        _ => {
            sqlx::query(
                "SELECT handle, elo, points,
                        (SELECT count(*) FROM calls c WHERE c.player_id = p.id AND c.settled_at IS NOT NULL) AS settled
                   FROM players p ORDER BY elo DESC, handle ASC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await
        }
    }
    .map_err(|err| {
        tracing::error!(error = %err, "leaderboard");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let players: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "handle": r.get::<String, _>("handle"),
                "elo": r.get::<f32, _>("elo"),
                "points": r.get::<i64, _>("points"),
                "settled_calls": r.get::<i64, _>("settled"),
            })
        })
        .collect();
    Ok(Json(json!({ "mode": mode, "players": players })))
}

// ── GET /v1/controversy + /v1/watchlist ────────────────────────────────────

/// Top articles by C_a right now (§5), with a war badge where a revert cycle is
/// active. Something even Wikipedia does not publicly surface.
pub async fn controversy(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Value>, StatusCode> {
    let mut con = state.redis.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let limit = q.limit.unwrap_or(20).clamp(1, 100);

    let pairs: Vec<(String, f64)> = redis::cmd("ZREVRANGE")
        .arg("pulse:controversy")
        .arg(0)
        .arg(limit - 1)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "controversy board");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut out = Vec::with_capacity(pairs.len());
    for (article, score) in pairs {
        let at_war: Option<String> = redis::cmd("GET")
            .arg(format!("pulse:war:{article}"))
            .query_async(&mut con)
            .await
            .ok()
            .flatten();
        out.push(json!({
            "article": article,
            "controversy": score,
            "edit_war": at_war.is_some(),
        }));
    }
    Ok(Json(json!({ "count": out.len(), "articles": out })))
}

/// Gate-1-only candidates — the public leading indicators a Surge bet is made on.
pub async fn watchlist(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Value>, StatusCode> {
    let mut con = state.redis.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let limit = q.limit.unwrap_or(20).clamp(1, 100);

    let pairs: Vec<(String, f64)> = redis::cmd("ZREVRANGE")
        .arg("pulse:watchlist")
        .arg(0)
        .arg(limit - 1)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items: Vec<Value> = pairs
        .into_iter()
        .map(|(article, ts_ms)| {
            json!({ "article": article, "seen_at_ms": ts_ms as i64 })
        })
        .collect();
    Ok(Json(json!({ "count": items.len(), "candidates": items })))
}

/// `GET /v1/incidents?article=` — the revert incidents behind a radar row.
#[derive(Debug, Deserialize)]
pub struct IncidentQuery {
    pub article: String,
    pub limit: Option<i64>,
}

pub async fn incidents(
    State(state): State<Arc<AppState>>,
    Query(q): Query<IncidentQuery>,
) -> Result<Json<Value>, StatusCode> {
    let pool = pool(&state)?;
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query(
        "SELECT reverter, reverted, rev_id, at FROM revert_incidents
          WHERE article = $1 ORDER BY at DESC LIMIT $2",
    )
    .bind(&q.article)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "reverter": r.get::<String, _>("reverter"),
                "reverted": r.get::<String, _>("reverted"),
                "rev_id": r.try_get::<Option<i64>, _>("rev_id").ok().flatten(),
                "at": r.get::<chrono::DateTime<chrono::Utc>, _>("at").to_rfc3339(),
            })
        })
        .collect();
    Ok(Json(json!({ "article": q.article, "incidents": items })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signature_verifies_only_for_its_own_player() {
        let sig = sign(42);
        assert!(verify(42, &sig));
        assert!(!verify(43, &sig), "signature must be bound to the id");
    }

    #[test]
    fn a_tampered_signature_is_rejected() {
        let mut sig = sign(7);
        // Flip one hex nibble.
        let last = sig.pop().unwrap();
        sig.push(if last == 'a' { 'b' } else { 'a' });
        assert!(!verify(7, &sig));
        assert!(!verify(7, ""), "empty signature must fail");
        assert!(!verify(7, "short"), "length mismatch must fail");
    }

    fn headers_with(cookie: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, cookie.parse().unwrap());
        h
    }

    #[test]
    fn extracts_a_player_from_a_valid_cookie() {
        let cookie = format!("pulse_player=99.{}", sign(99));
        assert_eq!(player_from_cookies(&headers_with(&cookie)), Some(99));
    }

    #[test]
    fn finds_the_cookie_among_others() {
        let cookie = format!("theme=dark; pulse_player=5.{}; other=1", sign(5));
        assert_eq!(player_from_cookies(&headers_with(&cookie)), Some(5));
    }

    #[test]
    fn forged_and_malformed_cookies_yield_no_player() {
        for raw in [
            "pulse_player=99.deadbeef",
            "pulse_player=99",
            "pulse_player=",
            "pulse_player=abc.def",
            "unrelated=1",
        ] {
            assert_eq!(player_from_cookies(&headers_with(raw)), None, "{raw}");
        }
        assert_eq!(player_from_cookies(&HeaderMap::new()), None);
    }
}
