//! Pulse ingest — holds the Wikimedia SSE connection open 24/7.
//!
//! Phase 0 scope: a supervised long-running process that proves its env
//! wiring and stays up across container cycles. The SSE client, the raw
//! append and the `Last-Event-ID` resume land in Phase 1.
//!
//! This binary must never exit on its own: it runs as a simple-mode Zerops
//! service, so a process that returns would be restarted in a loop.

use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::config::init_tracing("ingest");

    let stream_url = common::config::stream_url();
    let cache_wired = common::config::valkey_url().is_ok();
    tracing::info!(%stream_url, cache_wired, "ingest configured");

    if !cache_wired {
        // Not fatal yet — Phase 1 makes Valkey required. Surfacing it now
        // means a wiring mistake is visible before it can cost stream data.
        tracing::warn!("VALKEY_URL is not set — the bus is unavailable");
    }

    let mut ticks: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        ticks += 1;
        tracing::info!(ticks, "ingest idle — awaiting Phase 1 firehose client");
    }
}
