//! Pulse ingest — holds the Wikimedia SSE connection open 24/7.
//!
//! ARCHITECTURE.md §2 backpressure rule: this service does *only* parse →
//! raw-append → XADD. All computation lives downstream in the detector, so the
//! hot path can never block on it.
//!
//! The gapless guarantee: the SSE `id:` of the last event we durably handled is
//! persisted to Valkey; on boot we send it back as `Last-Event-ID` and the
//! Kafka-backed stream replays from that position. Kill the container, restart
//! it, and the gap closes — which is the Phase 1 demo.

mod raw;
mod sse;

use std::time::Duration;

use anyhow::{Context, Result};
use common::keys;
use common::RcEvent;
use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;

/// How many events between durability checkpoints (PLAN.md Phase 1).
/// Also the raw-log flush batch, so the pointer never advances past data we
/// haven't fsynced: flush first, then move the pointer.
const CHECKPOINT_EVERY: u64 = 200;

/// Backoff bounds for reconnecting to the firehose.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Trim the bus so a stopped detector can't grow Valkey without bound.
/// At ~10 ev/s this is roughly a 5-hour buffer.
const BUS_MAXLEN: usize = 200_000;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::init_tracing("ingest");

    let stream_url = common::config::stream_url();
    let valkey_url = common::config::valkey_url().map_err(anyhow::Error::msg)?;
    let raw_dir = common::config::optional("RAW_DIR", "raw");

    let client = redis::Client::open(valkey_url).context("opening valkey client")?;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .context("connecting to valkey")?;

    let mut log = raw::RawLog::open(std::path::Path::new(&raw_dir))?;

    // The resume pointer. None on a cold start.
    let mut resume_from: Option<String> = redis::cmd("GET")
        .arg(keys::INGEST_LAST_EVENT_ID)
        .query_async(&mut con)
        .await
        .context("reading resume pointer")?;

    match &resume_from {
        Some(id) => tracing::info!(
            resume_from = %truncate(id, 120),
            "resuming firehose from persisted Last-Event-ID"
        ),
        None => tracing::info!("cold start — no resume pointer, joining the live edge"),
    }

    let http = reqwest::Client::builder()
        // Wikimedia asks consumers to identify themselves.
        .user_agent("Pulse/0.1 (Zerops Challenge; Wikimedia EventStreams consumer)")
        .connect_timeout(Duration::from_secs(15))
        // No overall timeout: this request is meant to never end.
        .build()
        .context("building http client")?;

    let mut backoff = BACKOFF_MIN;
    loop {
        let started = std::time::Instant::now();
        match consume(&http, &stream_url, &mut con, &mut log, &mut resume_from).await {
            Ok(()) => tracing::warn!("firehose closed cleanly — reconnecting"),
            Err(err) => tracing::error!(error = %err, "firehose connection failed"),
        }

        // Always flush what we hold before sleeping; a crash must not lose it.
        let buffered = log.pending();
        match log.flush() {
            Ok(_) if buffered > 0 => {
                tracing::info!(buffered, "flushed buffered events after disconnect")
            }
            Ok(_) => {}
            Err(err) => tracing::error!(error = %err, "flushing raw log after disconnect"),
        }

        // A connection that lasted a while is not a failing connection: reset
        // the backoff so a nightly blip doesn't leave us at a 30s cadence.
        if started.elapsed() > Duration::from_secs(60) {
            backoff = BACKOFF_MIN;
        }
        tracing::info!(backoff_secs = backoff.as_secs(), "reconnecting");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Hold one connection for as long as it lives, appending everything it yields.
async fn consume(
    http: &reqwest::Client,
    stream_url: &str,
    con: &mut MultiplexedConnection,
    log: &mut raw::RawLog,
    resume_from: &mut Option<String>,
) -> Result<()> {
    let mut req = http.get(stream_url).header("Accept", "text/event-stream");
    if let Some(id) = resume_from.as_deref() {
        req = req.header("Last-Event-ID", id);
    }

    let resp = req.send().await.context("opening SSE request")?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("firehose returned HTTP {status}");
    }
    tracing::info!(%status, "firehose connected");

    // Anything already on the stream when we reconnect is gap replay. We call
    // it done at the first event whose own timestamp is at or after connect
    // time, and report the span we recovered.
    let connected_at = chrono::Utc::now();
    let mut replayed: u64 = 0;
    let mut replay_first: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut replay_done = resume_from.is_none();

    let mut parser = sse::Parser::new();
    let mut body = resp.bytes_stream();
    let mut since_checkpoint: u64 = 0;
    let mut kept: u64 = 0;
    let mut seen: u64 = 0;
    let mut pending_id: Option<String> = None;

    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("reading firehose chunk")?;
        for frame in parser.feed(&chunk) {
            // Keep-alive comments produce no data.
            if frame.data.is_empty() {
                continue;
            }
            seen += 1;

            // The id advances even for events we filter out — otherwise a long
            // run of uninteresting events would replay forever after a restart.
            if let Some(id) = frame.id.clone() {
                pending_id = Some(id);
            }

            let event: RcEvent = match serde_json::from_str(&frame.data) {
                Ok(ev) => ev,
                Err(err) => {
                    // A shape we don't model must never kill ingest.
                    tracing::debug!(error = %err, "unparseable event, skipped");
                    continue;
                }
            };

            if !replay_done {
                match event.meta.dt.as_deref().and_then(parse_dt) {
                    Some(dt) if dt < connected_at => {
                        replayed += 1;
                        replay_first.get_or_insert(dt);
                    }
                    Some(dt) => {
                        report_replay(replayed, replay_first, dt);
                        replay_done = true;
                    }
                    None => {}
                }
            }

            // ARCHITECTURE.md §2: filter at the edge, compute downstream.
            if !event.is_interesting() {
                continue;
            }

            log.push(&frame.data);
            kept += 1;
            since_checkpoint += 1;

            redis::cmd("XADD")
                .arg(keys::BUS_EDITS)
                .arg("MAXLEN")
                .arg("~")
                .arg(BUS_MAXLEN)
                .arg("*")
                .arg("payload")
                .arg(&frame.data)
                .query_async::<()>(con)
                .await
                .context("XADD to bus")?;

            if since_checkpoint >= CHECKPOINT_EVERY {
                checkpoint(con, log, pending_id.as_deref()).await?;
                *resume_from = pending_id.clone();
                since_checkpoint = 0;
                tracing::info!(seen, kept, raw_lines = log.written, "checkpoint");
            }
        }
    }

    // Stream ended: persist what we have so the next boot resumes correctly.
    checkpoint(con, log, pending_id.as_deref()).await?;
    *resume_from = pending_id;
    tracing::info!(seen, kept, "firehose stream ended");
    Ok(())
}

/// Durability order matters: fsync the raw log FIRST, then advance the pointer.
/// Reversing it would let a crash skip events that were never written.
async fn checkpoint(
    con: &mut MultiplexedConnection,
    log: &mut raw::RawLog,
    last_id: Option<&str>,
) -> Result<()> {
    log.flush().context("flushing raw log")?;
    if let Some(id) = last_id {
        redis::cmd("SET")
            .arg(keys::INGEST_LAST_EVENT_ID)
            .arg(id)
            .query_async::<()>(con)
            .await
            .context("persisting resume pointer")?;
    }
    Ok(())
}

fn report_replay(
    replayed: u64,
    first: Option<chrono::DateTime<chrono::Utc>>,
    now_at: chrono::DateTime<chrono::Utc>,
) {
    if replayed == 0 {
        tracing::info!("resumed with no gap — caught up at the live edge");
        return;
    }
    let span = first.map(|f| (now_at - f).num_seconds()).unwrap_or_default();
    // This line is the evidence for the kill-the-container demo.
    tracing::info!(
        replayed_events = replayed,
        gap_seconds = span,
        "GAP REPLAY COMPLETE — recovered events missed while down, zero loss"
    );
}

fn parse_dt(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_streams_own_timestamp_format() {
        let dt = parse_dt("2026-08-09T05:00:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-08-09T05:00:00+00:00");
        assert!(parse_dt("not-a-date").is_none());
    }

    #[test]
    fn truncate_leaves_short_ids_alone() {
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("abcdefghijk", 3), "abc…");
    }
}
