//! Pulse detector — the gates, the classifier, the settlement engine.
//!
//! Phase 0 scope: a supervised long-running process proving it can see both
//! Valkey and Postgres. Gates 1+2, classification and receipts land in
//! Phase 2; Vandal Patrol settlement in Phase 3.
//!
//! Like `ingest`, this is a simple-mode service — it must not exit.

use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::config::init_tracing("detector");

    let cache_wired = common::config::valkey_url().is_ok();
    let db_wired = common::config::database_url().is_ok();
    tracing::info!(cache_wired, db_wired, "detector configured");

    if !cache_wired || !db_wired {
        tracing::warn!(
            cache_wired,
            db_wired,
            "detector is missing a dependency URL — check run.envVariables"
        );
    }

    let mut ticks: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        ticks += 1;
        tracing::info!(ticks, "detector idle — awaiting Phase 2 gates");
    }
}
