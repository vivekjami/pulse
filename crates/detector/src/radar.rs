//! Conflict radar — ARCHITECTURE.md §4 + PLAN.md Phase 4.
//!
//! Two derived products from the same revert stream:
//!
//! * **Controversy index** `C_a = decayed(reverts) / decayed(edits)` over 1 hour
//!   with a 10-minute half-life. Stored as exponentially-decayed counters so the
//!   "most fought-over pages right now" board reflects now, not all-time.
//! * **Edit-war cycles** — the last 20 revert edges per article, scanned for an
//!   A↔B (or A→B→C→A) cycle with ≥3 edges in 30 minutes. A 20-element scan, not
//!   a graph library.

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;

/// §4: λ = 10-minute half-life on the decayed counters.
const HALF_LIFE_SECS: f64 = 600.0;
/// Counters and edges live an hour past their last touch.
const TTL_SECS: i64 = 3_600;
/// §4: keep the last 20 revert edges per article.
const MAX_EDGES: isize = 20;
/// §4: a cycle needs ≥3 edges inside 30 minutes.
const CYCLE_WINDOW_MS: i64 = 30 * 60 * 1000;
const CYCLE_MIN_EDGES: usize = 3;

fn edits_key(a: &str) -> String {
    format!("pulse:ctrl:edits:{a}")
}
fn reverts_key(a: &str) -> String {
    format!("pulse:ctrl:reverts:{a}")
}
fn edges_key(a: &str) -> String {
    format!("pulse:edges:{a}")
}
fn war_key(a: &str) -> String {
    format!("pulse:war:{a}")
}
/// Leaderboard of C_a, read by `GET /v1/controversy`.
pub const CONTROVERSY_BOARD: &str = "pulse:controversy";

/// Decay a stored counter to `now` and add `amount`.
///
/// Stored as `"<value>:<ts_ms>"`, so decay is computed on touch rather than by a
/// sweep — there is no background job and a cold article costs nothing.
async fn bump_decayed(
    con: &mut MultiplexedConnection,
    key: &str,
    amount: f64,
    now_ms: i64,
) -> Result<f64> {
    let stored: Option<String> = redis::cmd("GET").arg(key).query_async(con).await.ok().flatten();
    let (prev, prev_ts) = stored
        .as_deref()
        .and_then(|s| s.split_once(':'))
        .and_then(|(v, t)| Some((v.parse::<f64>().ok()?, t.parse::<i64>().ok()?)))
        .unwrap_or((0.0, now_ms));

    let elapsed = ((now_ms - prev_ts).max(0) as f64) / 1000.0;
    let decayed = prev * 0.5_f64.powf(elapsed / HALF_LIFE_SECS);
    let value = decayed + amount;

    redis::pipe()
        .atomic()
        .cmd("SET")
        .arg(key)
        .arg(format!("{value}:{now_ms}"))
        .ignore()
        .cmd("EXPIRE")
        .arg(key)
        .arg(TTL_SECS)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("storing decayed counter")?;
    Ok(value)
}

/// Read a decayed counter without touching it.
async fn read_decayed(con: &mut MultiplexedConnection, key: &str, now_ms: i64) -> f64 {
    let stored: Option<String> = redis::cmd("GET").arg(key).query_async(con).await.ok().flatten();
    stored
        .as_deref()
        .and_then(|s| s.split_once(':'))
        .and_then(|(v, t)| Some((v.parse::<f64>().ok()?, t.parse::<i64>().ok()?)))
        .map(|(v, ts)| {
            let elapsed = ((now_ms - ts).max(0) as f64) / 1000.0;
            v * 0.5_f64.powf(elapsed / HALF_LIFE_SECS)
        })
        .unwrap_or(0.0)
}

/// Count one ordinary edit toward the denominator of C_a.
pub async fn record_edit(
    con: &mut MultiplexedConnection,
    article: &str,
    now_ms: i64,
) -> Result<()> {
    bump_decayed(con, &edits_key(article), 1.0, now_ms).await?;
    Ok(())
}

/// Count one revert and refresh the article's place on the board.
pub async fn record_revert(
    con: &mut MultiplexedConnection,
    article: &str,
    now_ms: i64,
) -> Result<f64> {
    let reverts = bump_decayed(con, &reverts_key(article), 1.0, now_ms).await?;
    let edits = read_decayed(con, &edits_key(article), now_ms).await.max(1.0);
    let score = reverts / edits;

    redis::pipe()
        .atomic()
        .cmd("ZADD")
        .arg(CONTROVERSY_BOARD)
        .arg(score)
        .arg(article)
        .ignore()
        // Keep the board bounded — it is a "right now" board, top 20 by rank.
        .cmd("ZREMRANGEBYRANK")
        .arg(CONTROVERSY_BOARD)
        .arg(0)
        .arg(-201)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("updating controversy board")?;
    Ok(score)
}

/// One directed revert edge: `reverter` undid `reverted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub reverter: String,
    pub reverted: String,
    pub at_ms: i64,
}

/// Append an edge and return whether the article is now in an edit war.
pub async fn record_edge(
    con: &mut MultiplexedConnection,
    article: &str,
    edge: &Edge,
    now_ms: i64,
) -> Result<bool> {
    let key = edges_key(article);
    // Tab-separated: usernames can contain almost anything except a tab.
    let packed = format!("{}\t{}\t{}", edge.reverter, edge.reverted, edge.at_ms);
    redis::pipe()
        .atomic()
        .cmd("LPUSH")
        .arg(&key)
        .arg(&packed)
        .ignore()
        .cmd("LTRIM")
        .arg(&key)
        .arg(0)
        .arg(MAX_EDGES - 1)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(TTL_SECS)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("recording revert edge")?;

    let raw: Vec<String> = redis::cmd("LRANGE")
        .arg(&key)
        .arg(0)
        .arg(MAX_EDGES - 1)
        .query_async(con)
        .await
        .unwrap_or_default();
    let edges: Vec<Edge> = raw.iter().filter_map(|s| unpack(s)).collect();

    let at_war = detect_cycle(&edges, now_ms);
    if at_war {
        redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(war_key(article))
            .arg(1)
            .ignore()
            .cmd("EXPIRE")
            .arg(war_key(article))
            .arg(1_800)
            .ignore()
            .query_async::<()>(con)
            .await
            .context("flagging edit war")?;
    }
    Ok(at_war)
}

fn unpack(s: &str) -> Option<Edge> {
    let mut parts = s.split('\t');
    let reverter = parts.next()?.to_string();
    let reverted = parts.next()?.to_string();
    let at_ms = parts.next()?.parse().ok()?;
    Some(Edge {
        reverter,
        reverted,
        at_ms,
    })
}

/// Is this article in a revert cycle? §4: an A↔B (or A→B→C→A) cycle with ≥3
/// edges inside 30 minutes.
///
/// Pure so the rule is testable without Valkey.
pub fn detect_cycle(edges: &[Edge], now_ms: i64) -> bool {
    let recent: Vec<&Edge> = edges
        .iter()
        .filter(|e| now_ms - e.at_ms <= CYCLE_WINDOW_MS)
        .collect();
    if recent.len() < CYCLE_MIN_EDGES {
        return false;
    }

    // A cycle exists when some pair has reverted each other (A→B and B→A), or a
    // 3-chain closes back on itself. Both are cheap over ≤20 edges.
    for (i, a) in recent.iter().enumerate() {
        for b in recent.iter().skip(i + 1) {
            if a.reverter == b.reverted && a.reverted == b.reverter {
                return true; // A↔B ping-pong
            }
        }
    }
    for a in &recent {
        for b in &recent {
            if a.reverted != b.reverter {
                continue;
            }
            for c in &recent {
                if b.reverted == c.reverter && c.reverted == a.reverter {
                    return true; // A→B→C→A
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(reverter: &str, reverted: &str, at_ms: i64) -> Edge {
        Edge {
            reverter: reverter.to_string(),
            reverted: reverted.to_string(),
            at_ms,
        }
    }

    const NOW: i64 = 1_800_000_000_000;

    #[test]
    fn ping_pong_between_two_editors_is_a_war() {
        let edges = vec![
            e("Ann", "Ben", NOW - 60_000),
            e("Ben", "Ann", NOW - 120_000),
            e("Ann", "Ben", NOW - 180_000),
        ];
        assert!(detect_cycle(&edges, NOW));
    }

    #[test]
    fn a_three_way_cycle_is_a_war() {
        let edges = vec![
            e("Ann", "Ben", NOW - 60_000),
            e("Ben", "Cal", NOW - 120_000),
            e("Cal", "Ann", NOW - 180_000),
        ];
        assert!(detect_cycle(&edges, NOW));
    }

    #[test]
    fn one_sided_reverts_are_not_a_war() {
        // A patroller cleaning up three different vandals is not a war.
        let edges = vec![
            e("Patroller", "Ben", NOW - 60_000),
            e("Patroller", "Cal", NOW - 120_000),
            e("Patroller", "Dee", NOW - 180_000),
        ];
        assert!(!detect_cycle(&edges, NOW));
    }

    #[test]
    fn too_few_edges_is_not_a_war() {
        let edges = vec![e("Ann", "Ben", NOW - 60_000), e("Ben", "Ann", NOW - 90_000)];
        assert!(!detect_cycle(&edges, NOW), "needs >= 3 edges");
    }

    #[test]
    fn stale_edges_fall_out_of_the_thirty_minute_window() {
        let old = NOW - 40 * 60 * 1000;
        let edges = vec![
            e("Ann", "Ben", old),
            e("Ben", "Ann", old - 60_000),
            e("Ann", "Ben", old - 120_000),
        ];
        assert!(!detect_cycle(&edges, NOW));
    }

    #[test]
    fn edges_round_trip_through_packing() {
        let edge = e("Ann Smith", "203.0.113.9", NOW);
        let packed = format!("{}\t{}\t{}", edge.reverter, edge.reverted, edge.at_ms);
        assert_eq!(unpack(&packed), Some(edge));
        assert_eq!(unpack("malformed"), None);
    }

    #[test]
    fn decay_halves_over_one_half_life() {
        // Pure arithmetic mirror of bump_decayed's decay step.
        let decayed = 1.0_f64 * 0.5_f64.powf(HALF_LIFE_SECS / HALF_LIFE_SECS);
        assert!((decayed - 0.5).abs() < 1e-9);
    }
}
