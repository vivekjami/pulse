//! Hot state in Valkey — ARCHITECTURE.md §3.1.
//!
//! Deviation from §3.1, deliberate and load-bearing: the distinct-editor key is
//! a HASH (`HINCRBY user 1`) rather than a SET (`SADD user`). §4's Gate 2 needs
//! `top_editor_share`, which is per-editor *counts*; a set only yields
//! cardinality. The hash is a superset — cardinality still comes from `HLEN` —
//! so nothing in §3.1 is lost. (PLAN.md lists dropping the top-share check as an
//! allowed cut line; implementing it properly is the better trade.)
//!
//! Memory discipline (§3.1): only articles with ≥2 edits in the window get a
//! window key, everything expires, and the expected working set is low thousands
//! of keys.

use anyhow::{Context, Result};
use common::keys;
use redis::aio::MultiplexedConnection;

use crate::gates::{EditorTally, Tunables};

/// Per-article evidence and counters read back for the gates.
pub struct ArticleState {
    /// Edits inside the rate window.
    pub window_edits: f64,
    /// Per-editor counts inside the window (bots never enter).
    pub tally: EditorTally,
    /// The article's own EWMA baseline, edits per minute.
    pub ewma: f64,
}

/// Key for the category evidence set. Not in §3.1, but §4's classification
/// consumes `categorize` events, so their categories must be held somewhere
/// until the article's burst confirms.
fn cats_key(article: &str) -> String {
    format!("pulse:cats:{article}")
}

/// Sample edit comments, kept for the receipt's evidence bundle and the event
/// card (§4 evidence, PLAN.md Phase 2 "sample comments").
fn comments_key(article: &str) -> String {
    format!("pulse:cmts:{article}")
}

/// Cooldown marker so one long-running burst yields one receipt, not hundreds.
fn cooldown_key(article: &str) -> String {
    format!("pulse:confirmed:{article}")
}

/// EWMA bookkeeping: which 1-minute bucket the stored average last absorbed.
const EWMA_BUCKET_HASH: &str = "pulse:ewma:bucket";
/// The global baseline, smoothed over ~1 hour of 1-minute buckets.
const GLOBAL_EWMA_KEY: &str = "pulse:global:ewma";

/// Count one event toward the global stream rate (§3.1: EXPIRE 10m).
pub async fn bump_global_rate(
    con: &mut MultiplexedConnection,
    now_ms: i64,
) -> Result<()> {
    let key = keys::global_rate(keys::bucket_1m(now_ms));
    redis::pipe()
        .atomic()
        .cmd("INCR")
        .arg(&key)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(600)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("bumping global rate")?;
    Ok(())
}

/// Read the previous minute's global count and the smoothed baseline, updating
/// the baseline once per minute.
///
/// The previous bucket is used rather than the current one because the current
/// minute is still filling and would always read low.
pub async fn global_rate_and_baseline(
    con: &mut MultiplexedConnection,
    now_ms: i64,
    t: &Tunables,
) -> Result<(f64, f64)> {
    let prev_bucket = keys::bucket_1m(now_ms) - 1;
    let key = keys::global_rate(prev_bucket);
    let count: Option<f64> = redis::cmd("GET").arg(&key).query_async(con).await.ok();
    let rate = count.unwrap_or(0.0);

    let stored: Option<String> = redis::cmd("GET")
        .arg(GLOBAL_EWMA_KEY)
        .query_async(con)
        .await
        .ok()
        .flatten();

    // Stored as "<value>:<bucket>" so the baseline absorbs each minute once.
    let (mut baseline, last_bucket) = stored
        .as_deref()
        .and_then(|s| s.split_once(':'))
        .and_then(|(v, b)| Some((v.parse::<f64>().ok()?, b.parse::<i64>().ok()?)))
        .unwrap_or((0.0, i64::MIN));

    if rate > 0.0 && prev_bucket > last_bucket {
        baseline = if baseline <= 0.0 {
            rate // seed on first observation instead of crawling up from zero
        } else {
            crate::gates::ewma_step(baseline, rate, t.global_alpha)
        };
        let _: Result<(), _> = redis::cmd("SET")
            .arg(GLOBAL_EWMA_KEY)
            .arg(format!("{baseline}:{prev_bucket}"))
            .query_async::<()>(con)
            .await;
    }

    Ok((rate, baseline))
}

/// Record one non-bot article edit into the window and the editor tally.
pub async fn record_edit(
    con: &mut MultiplexedConnection,
    article: &str,
    user: &str,
    member: &str,
    comment: &str,
    now_ms: i64,
    t: &Tunables,
) -> Result<()> {
    let win = keys::window(article);
    let eds = keys::editors(article, keys::bucket_10m(now_ms));
    let cmts = comments_key(article);
    let cutoff = now_ms - t.window_secs * 1000;

    let mut pipe = redis::pipe();
    pipe.atomic()
        // §3.1: ZADD ts_ms → event uuid, trimmed by ZREMRANGEBYSCORE.
        .cmd("ZADD")
        .arg(&win)
        .arg(now_ms)
        .arg(member)
        .ignore()
        .cmd("ZREMRANGEBYSCORE")
        .arg(&win)
        .arg("-inf")
        .arg(cutoff)
        .ignore()
        .cmd("EXPIRE")
        .arg(&win)
        .arg(t.window_secs)
        .ignore()
        // Per-editor counts — see the module note on HASH vs SET.
        .cmd("HINCRBY")
        .arg(&eds)
        .arg(user)
        .arg(1)
        .ignore()
        .cmd("EXPIRE")
        .arg(&eds)
        .arg(1800)
        .ignore();

    if !comment.is_empty() {
        pipe.cmd("LPUSH")
            .arg(&cmts)
            .arg(comment)
            .ignore()
            .cmd("LTRIM")
            .arg(&cmts)
            .arg(0)
            .arg(9)
            .ignore()
            .cmd("EXPIRE")
            .arg(&cmts)
            .arg(t.window_secs)
            .ignore();
    }

    pipe.query_async::<()>(con)
        .await
        .context("recording edit into window")?;
    Ok(())
}

/// Note that an article was added to a category (§4 classification evidence).
pub async fn record_category(
    con: &mut MultiplexedConnection,
    article: &str,
    category: &str,
    t: &Tunables,
) -> Result<()> {
    let key = cats_key(article);
    redis::pipe()
        .atomic()
        .cmd("SADD")
        .arg(&key)
        .arg(category)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(t.window_secs)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("recording category")?;
    Ok(())
}

/// Read everything the gates need for one article, and advance its EWMA.
pub async fn load_state(
    con: &mut MultiplexedConnection,
    article: &str,
    now_ms: i64,
    t: &Tunables,
) -> Result<ArticleState> {
    let win = keys::window(article);
    let rate_cutoff = now_ms - t.rate_window_secs * 1000;

    let window_edits: f64 = redis::cmd("ZCOUNT")
        .arg(&win)
        .arg(rate_cutoff)
        .arg("+inf")
        .query_async::<i64>(con)
        .await
        .context("counting window")? as f64;

    let pairs: Vec<(String, u32)> = redis::cmd("HGETALL")
        .arg(keys::editors(article, keys::bucket_10m(now_ms)))
        .query_async::<std::collections::HashMap<String, u32>>(con)
        .await
        .context("reading editor tally")?
        .into_iter()
        .collect();

    let ewma = update_ewma(con, article, now_ms, t).await?;

    Ok(ArticleState {
        window_edits,
        tally: EditorTally::from_pairs(pairs),
        ewma,
    })
}

/// Advance the per-article EWMA at most once per 1-minute bucket (§4: α = 0.3).
///
/// The sample is the *previous* completed minute's edit count, so a burst in
/// progress never inflates its own baseline — which would mask the very anomaly
/// Gate 1 is looking for.
async fn update_ewma(
    con: &mut MultiplexedConnection,
    article: &str,
    now_ms: i64,
    t: &Tunables,
) -> Result<f64> {
    let bucket = keys::bucket_1m(now_ms);
    let prev_bucket = bucket - 1;

    let stored: Option<f64> = redis::cmd("HGET")
        .arg(keys::EWMA)
        .arg(article)
        .query_async(con)
        .await
        .ok()
        .flatten();
    let last: Option<i64> = redis::cmd("HGET")
        .arg(EWMA_BUCKET_HASH)
        .arg(article)
        .query_async(con)
        .await
        .ok()
        .flatten();

    let mut value = stored.unwrap_or(0.0);
    if last.map_or(true, |b| prev_bucket > b) {
        let win = keys::window(article);
        let from = prev_bucket * 60_000;
        let to = bucket * 60_000 - 1;
        let sample: i64 = redis::cmd("ZCOUNT")
            .arg(&win)
            .arg(from)
            .arg(to)
            .query_async(con)
            .await
            .unwrap_or(0);
        value = crate::gates::ewma_step(value, sample as f64, t.ewma_alpha);

        redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(keys::EWMA)
            .arg(article)
            .arg(value)
            .ignore()
            .cmd("HSET")
            .arg(EWMA_BUCKET_HASH)
            .arg(article)
            .arg(prev_bucket)
            .ignore()
            .query_async::<()>(con)
            .await
            .context("storing ewma")?;
    }
    Ok(value)
}

/// Evidence bundle attached to a receipt.
pub struct Evidence {
    pub categories: Vec<String>,
    pub comments: Vec<String>,
}

pub async fn load_evidence(
    con: &mut MultiplexedConnection,
    article: &str,
) -> Result<Evidence> {
    let categories: Vec<String> = redis::cmd("SMEMBERS")
        .arg(cats_key(article))
        .query_async(con)
        .await
        .unwrap_or_default();
    let comments: Vec<String> = redis::cmd("LRANGE")
        .arg(comments_key(article))
        .arg(0)
        .arg(9)
        .query_async(con)
        .await
        .unwrap_or_default();
    Ok(Evidence {
        categories,
        comments,
    })
}

/// Claim the right to confirm this article, returning false if already claimed.
/// `SET NX` makes this atomic, so a future partitioned detector can't
/// double-write the same receipt.
pub async fn claim_confirmation(
    con: &mut MultiplexedConnection,
    article: &str,
    t: &Tunables,
) -> Result<bool> {
    let claimed: Option<String> = redis::cmd("SET")
        .arg(cooldown_key(article))
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(t.cooldown_secs)
        .query_async(con)
        .await
        .context("claiming confirmation")?;
    Ok(claimed.is_some())
}
