//! Pulse API — REST + SSE fanout.
//!
//! Phase 0 scope: the health surface and a service banner, so the deploy
//! pipeline and the L7 route are proven before any stream logic exists.
//! `/v1/live` SSE and the receipts endpoints land in Phase 1 and 2.

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::config::init_tracing("api");

    // Wiring is reported, never printed: a missing DATABASE_URL is a
    // deploy-config bug we want visible in logs without leaking the value.
    let db_wired = common::config::database_url().is_ok();
    let cache_wired = common::config::valkey_url().is_ok();
    tracing::info!(db_wired, cache_wired, "cross-service env wiring");

    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz));

    let port = common::config::port();
    // 0.0.0.0, never loopback — the L7 balancer routes to the VXLAN IP.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "api listening");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Service banner. `zerops_verify` probes `GET /`, so this must return a real body.
async fn root() -> Json<Value> {
    Json(json!({
        "service": "pulse-api",
        "version": env!("CARGO_PKG_VERSION"),
        "phase": 0,
        "endpoints": ["/healthz"],
        "source": "https://stream.wikimedia.org/v2/stream/recentchange",
    }))
}

/// The Phase 0 exit criterion.
async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}
