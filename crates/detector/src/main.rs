//! Pulse detector — the gates, the classifier, the receipts ledger.
//!
//! Consumes `pulse:bus:edits` as a consumer group, so a crash or redeploy
//! resumes from pending entries rather than losing them (ARCHITECTURE.md §8).
//! Every confirmation writes a permanent row to `events` and publishes a
//! `confirmed` frame for the api to fan out.

mod classify;
mod gates;
mod state;

use std::time::Duration;

use anyhow::{Context, Result};
use common::keys;
use common::RcEvent;
use gates::Tunables;
use redis::aio::MultiplexedConnection;
use serde_json::json;
use sqlx::postgres::PgPool;

/// Batch size per XREADGROUP call.
const READ_COUNT: usize = 200;
/// How long to block waiting for new entries.
const READ_BLOCK_MS: usize = 2_000;
/// Consumer name within the group.
const CONSUMER: &str = "detector-1";

#[tokio::main]
async fn main() -> Result<()> {
    common::config::init_tracing("detector");

    let valkey_url = common::config::valkey_url().map_err(anyhow::Error::msg)?;
    let database_url = common::config::database_url().map_err(anyhow::Error::msg)?;
    let t = Tunables::from_env();
    tracing::info!(?t, "detector tunables");

    let pool = common::db::connect(&database_url, 4).await?;
    common::db::ensure_schema(&pool).await?;

    let client = redis::Client::open(valkey_url).context("opening valkey client")?;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .context("connecting to valkey")?;

    ensure_group(&mut con).await?;

    // Reclaim anything a previous container died holding (§8).
    let reclaimed = claim_stale(&mut con).await.unwrap_or(0);
    if reclaimed > 0 {
        tracing::info!(reclaimed, "reclaimed pending entries from a prior container");
    }

    let mut stats = Stats::default();
    loop {
        match tick(&mut con, &pool, &t, &mut stats).await {
            Ok(0) => {}
            Ok(n) => tracing::debug!(processed = n, "batch"),
            Err(err) => {
                tracing::error!(error = %err, "detector batch failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

#[derive(Default)]
struct Stats {
    seen: u64,
    windowed: u64,
    gate1: u64,
    gate2: u64,
    confirmed: u64,
    /// Events the stream gave us no usable timestamp for.
    undated: u64,
    last_report: Option<std::time::Instant>,
}

/// Create the consumer group, tolerating "already exists".
async fn ensure_group(con: &mut MultiplexedConnection) -> Result<()> {
    let res: Result<(), redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(keys::BUS_EDITS)
        .arg(keys::GROUP_DETECTOR)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(con)
        .await;
    match res {
        Ok(()) => tracing::info!(group = keys::GROUP_DETECTOR, "consumer group created"),
        Err(err) if err.to_string().contains("BUSYGROUP") => {
            tracing::info!(group = keys::GROUP_DETECTOR, "consumer group already exists")
        }
        Err(err) => return Err(err).context("creating consumer group"),
    }
    Ok(())
}

/// XAUTOCLAIM entries idle longer than a minute — a redeploy leaves pending
/// entries owned by a container that no longer exists.
async fn claim_stale(con: &mut MultiplexedConnection) -> Result<usize> {
    let reply: redis::Value = redis::cmd("XAUTOCLAIM")
        .arg(keys::BUS_EDITS)
        .arg(keys::GROUP_DETECTOR)
        .arg(CONSUMER)
        .arg(60_000)
        .arg("0-0")
        .arg("COUNT")
        .arg(500)
        .query_async(con)
        .await
        .context("xautoclaim")?;
    // Shape is [next_cursor, [entries...], [deleted...]]; we only need a count
    // for the log line, and the entries land in our pending set either way.
    if let redis::Value::Array(parts) = &reply {
        if let Some(redis::Value::Array(entries)) = parts.get(1) {
            return Ok(entries.len());
        }
    }
    Ok(0)
}

/// One XREADGROUP batch.
async fn tick(
    con: &mut MultiplexedConnection,
    pool: &PgPool,
    t: &Tunables,
    stats: &mut Stats,
) -> Result<usize> {
    let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(keys::GROUP_DETECTOR)
        .arg(CONSUMER)
        .arg("COUNT")
        .arg(READ_COUNT)
        .arg("BLOCK")
        .arg(READ_BLOCK_MS)
        .arg("STREAMS")
        .arg(keys::BUS_EDITS)
        .arg(">")
        .query_async(con)
        .await
        .context("XREADGROUP")?;

    let mut processed = 0usize;
    let mut ack_ids: Vec<String> = Vec::new();

    for stream in reply.keys {
        for mut entry in stream.ids {
            let id = std::mem::take(&mut entry.id);
            if let Some(v) = entry.map.remove("payload") {
                if let Ok(payload) = redis::from_redis_value::<String>(v) {
                    if let Err(err) = handle(con, pool, t, stats, &payload).await {
                        // A single malformed event must not stall the group.
                        tracing::debug!(error = %err, "event handling failed");
                    }
                }
            }
            ack_ids.push(id);
            processed += 1;
        }
    }

    if !ack_ids.is_empty() {
        let mut cmd = redis::cmd("XACK");
        cmd.arg(keys::BUS_EDITS).arg(keys::GROUP_DETECTOR);
        for id in &ack_ids {
            cmd.arg(id);
        }
        cmd.query_async::<i64>(con).await.context("XACK")?;
    }

    report(stats);
    Ok(processed)
}

fn report(stats: &mut Stats) {
    let due = stats
        .last_report
        .map_or(true, |t| t.elapsed() > Duration::from_secs(30));
    if due {
        stats.last_report = Some(std::time::Instant::now());
        tracing::info!(
            seen = stats.seen,
            windowed = stats.windowed,
            gate1_passed = stats.gate1,
            gate2_passed = stats.gate2,
            confirmed = stats.confirmed,
            undated = stats.undated,
            "detector throughput"
        );
    }
}

/// Process one raw event.
async fn handle(
    con: &mut MultiplexedConnection,
    pool: &PgPool,
    t: &Tunables,
    stats: &mut Stats,
    payload: &str,
) -> Result<()> {
    let ev: RcEvent = serde_json::from_str(payload).context("parsing event")?;
    stats.seen += 1;

    // Event time, not wall-clock: see RcEvent::event_time_ms. An event the
    // stream never timestamped is unusable for windowing, so it is counted and
    // dropped rather than smeared onto the current instant.
    let Some(now_ms) = ev.event_time_ms() else {
        stats.undated += 1;
        return Ok(());
    };

    // Every event counts toward the global rate, bots included — the whole
    // point of the normalizer is to see floods.
    state::bump_global_rate(con, now_ms).await?;

    // `categorize` events name the *category* in `title`; the article that moved
    // is in the comment. They are classification evidence, not window activity.
    if ev.kind == "categorize" {
        if let Some(article_title) = classify::categorized_article(&ev.comment) {
            let article = format!("{}:{}", ev.wiki, article_title);
            state::record_category(con, &article, &ev.title, t).await?;
        }
        return Ok(());
    }

    // §4: drop bots before they ever reach the windows.
    if ev.bot {
        return Ok(());
    }
    // Real-world events erupt on articles, not on talk or project pages.
    if !ev.is_main_namespace() {
        return Ok(());
    }

    let article = ev.article_key();
    // Window members must be unique per event; meta.id is the stream's own uuid.
    let member = ev
        .meta
        .id
        .clone()
        .unwrap_or_else(|| format!("{}-{}", now_ms, stats.seen));

    state::record_edit(con, &article, &ev.user, &member, &ev.comment, now_ms, t).await?;
    stats.windowed += 1;

    let st = state::load_state(con, &article, now_ms, t).await?;

    // §3.1: only articles with real activity are worth evaluating.
    if st.window_edits < 2.0 {
        return Ok(());
    }

    let (global_rate, global_baseline) = state::global_rate_and_baseline(con, now_ms, t).await?;
    let g1 = gates::gate1(st.window_edits, st.ewma, global_rate, global_baseline, t);
    if !g1.fired {
        return Ok(());
    }
    stats.gate1 += 1;
    let gate1_at = chrono::Utc::now();

    let g2 = gates::gate2(&st.tally, t);
    if !g2.fired {
        // Logged at debug so live tuning can watch what is being rejected and
        // why, which is how k1 gets calibrated (PLAN.md budgets 45 min for it).
        tracing::debug!(
            %article,
            editors = g2.distinct_editors,
            registered = g2.registered_editors,
            top_share = g2.top_share,
            "gate1 passed, gate2 rejected"
        );
        return Ok(());
    }
    stats.gate2 += 1;
    let gate2_at = chrono::Utc::now();

    // One receipt per burst.
    if !state::claim_confirmation(con, &article, t).await? {
        return Ok(());
    }

    let evidence = state::load_evidence(con, &article).await?;
    let kind = classify::classify(&evidence.categories, &evidence.comments);
    let detected_at = chrono::Utc::now();

    let evidence_json = json!({
        "categories": evidence.categories,
        "sample_comments": evidence.comments,
        "gate1": {
            "window_edits": g1.window_edits,
            "anomaly": g1.anomaly,
            "threshold": g1.threshold,
            "global_factor": g1.global_factor,
        },
        "gate2": {
            "distinct_editors": g2.distinct_editors,
            "registered_editors": g2.registered_editors,
            "top_editor_share": g2.top_share,
        },
        "editors": st.tally.counts,
        "article_ewma": st.ewma,
        "last_rev_id": ev.revision.and_then(|r| r.new),
        "title_url": ev.title_url,
        "wiki": ev.wiki,
        "title": ev.title,
    });

    let row: (i64,) = sqlx::query_as(
        "INSERT INTO events
           (article, kind, detected_at, gate1_at, gate2_at, peak_rate, distinct_eds, evidence)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id",
    )
    .bind(&article)
    .bind(kind.as_str())
    .bind(detected_at)
    .bind(gate1_at)
    .bind(gate2_at)
    .bind(g1.window_edits as f32)
    .bind(g2.distinct_editors as i32)
    .bind(&evidence_json)
    .fetch_one(pool)
    .await
    .context("inserting receipt")?;

    let frame = json!({
        "id": row.0,
        "article": article,
        "kind": kind.as_str(),
        "detected_at": detected_at.to_rfc3339(),
        "distinct_eds": g2.distinct_editors,
        "peak_rate": g1.window_edits,
        "wiki": ev.wiki,
        "title": ev.title,
        "title_url": ev.title_url,
        "sample_comments": evidence.comments.iter().take(3).collect::<Vec<_>>(),
    });
    redis::cmd("XADD")
        .arg(keys::BUS_CONFIRMED)
        .arg("MAXLEN")
        .arg("~")
        .arg(10_000)
        .arg("*")
        .arg("payload")
        .arg(frame.to_string())
        .query_async::<()>(con)
        .await
        .context("publishing confirmation")?;

    stats.confirmed += 1;
    tracing::info!(
        id = row.0,
        %article,
        kind = kind.as_str(),
        editors = g2.distinct_editors,
        window_edits = g1.window_edits,
        anomaly = format!("{:.1}x", g1.anomaly),
        "CONFIRMED BURST"
    );
    Ok(())
}
