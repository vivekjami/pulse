//! Postgres access shared by detector and api.

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// ARCHITECTURE.md §3.2, embedded so the binary carries its own schema.
const SCHEMA: &str = include_str!("schema.sql");

/// Connect with a small pool — three services share one NON_HA Postgres.
pub async fn connect(url: &str, max_conns: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(url)
        .await
        .context("connecting to postgres")
}

/// Apply the schema. Idempotent (`CREATE TABLE IF NOT EXISTS` throughout), so
/// whichever service boots first wins and the other is a no-op.
pub async fn ensure_schema(pool: &PgPool) -> Result<()> {
    // Executed as one multi-statement batch.
    sqlx::raw_sql(SCHEMA)
        .execute(pool)
        .await
        .context("applying schema")?;
    tracing::info!("schema ensured");
    Ok(())
}
