//! Settlement — PLAN.md Phase 3 (Vandal Patrol) and Phase 5 (Call the Surge).
//!
//! "Reality grades you": no invented mechanics. A Vandal Patrol call is settled
//! by whether a real revert landed on that article inside the deadline, and a
//! Surge bet by whether the detector's own gates confirmed the article. Both
//! outcomes arrive on the same stream the player was looking at.

use anyhow::{Context, Result};
use common::keys;
use redis::aio::ConnectionManager;
use sqlx::postgres::PgPool;
use sqlx::Row;

/// §Phase 3: K = 32, expected score against a fixed 1200 "house".
pub const ELO_K: f64 = 32.0;
pub const HOUSE_ELO: f64 = 1200.0;

/// Strong reverts, per article, so settlement can ask "was this reverted?"
/// without scanning Postgres. Score is the revert's event time.
fn reverts_key(article: &str) -> String {
    format!("pulse:reverts:{article}")
}

/// Record a strong revert for settlement lookup. Member encodes the reverted
/// revision id when the comment named one.
pub async fn record_revert_for_settlement(
    con: &mut ConnectionManager,
    article: &str,
    rev_id: Option<i64>,
    now_ms: i64,
) -> Result<()> {
    let key = reverts_key(article);
    let member = match rev_id {
        Some(id) => format!("rev:{id}"),
        // Uniquify so two unnamed reverts don't collapse into one ZSET member.
        None => format!("any:{now_ms}"),
    };
    redis::pipe()
        .atomic()
        .cmd("ZADD")
        .arg(&key)
        .arg(now_ms)
        .arg(member)
        .ignore()
        // A patrol deadline is 10 minutes; an hour of history is ample.
        .cmd("ZREMRANGEBYSCORE")
        .arg(&key)
        .arg("-inf")
        .arg(now_ms - 3_600_000)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(3_600)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("recording revert for settlement")?;
    Ok(())
}

/// Expected score for `player` against the house (standard Elo).
pub fn expected_score(player_elo: f64) -> f64 {
    1.0 / (1.0 + 10_f64.powf((HOUSE_ELO - player_elo) / 400.0))
}

/// New rating after one settled call.
pub fn elo_update(player_elo: f64, correct: bool) -> f64 {
    let actual = if correct { 1.0 } else { 0.0 };
    player_elo + ELO_K * (actual - expected_score(player_elo))
}

/// Was the player right? Verdict `true` means "vandalism", which reality
/// confirms by reverting the edit.
pub fn call_is_correct(verdict: bool, was_reverted: bool) -> bool {
    verdict == was_reverted
}

/// Settle every Vandal Patrol call whose deadline has passed.
///
/// Returns how many were settled. Driven from the detector loop rather than a
/// cron so settlement latency is seconds, which is what makes the loop feel
/// like the stream grading you.
pub async fn settle_due_calls(
    con: &mut ConnectionManager,
    pool: &PgPool,
    now_ms: i64,
) -> Result<usize> {
    // Due = deadline at or before now.
    let due: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(keys::SETTLE_QUEUE)
        .arg("-inf")
        .arg(now_ms)
        .arg("LIMIT")
        .arg(0)
        .arg(100)
        .query_async(con)
        .await
        .unwrap_or_default();

    let mut settled = 0usize;
    for call_id_str in due {
        let Ok(call_id) = call_id_str.parse::<i64>() else {
            // Unparseable member: drop it rather than retry forever.
            let _: Result<i64, _> = redis::cmd("ZREM")
                .arg(keys::SETTLE_QUEUE)
                .arg(&call_id_str)
                .query_async(con)
                .await;
            continue;
        };

        let row = sqlx::query(
            "SELECT c.id, c.player_id, c.article, c.rev_id, c.verdict, c.called_at, c.deadline,
                    p.elo
               FROM calls c JOIN players p ON p.id = c.player_id
              WHERE c.id = $1 AND c.settled_at IS NULL",
        )
        .bind(call_id)
        .fetch_optional(pool)
        .await
        .context("loading call")?;

        let Some(row) = row else {
            // Already settled or gone — stop tracking it.
            let _: Result<i64, _> = redis::cmd("ZREM")
                .arg(keys::SETTLE_QUEUE)
                .arg(&call_id_str)
                .query_async(con)
                .await;
            continue;
        };

        let article: String = row.get("article");
        let rev_id: i64 = row.get("rev_id");
        let verdict: bool = row.get("verdict");
        let elo: f32 = row.get("elo");
        let called_at: chrono::DateTime<chrono::Utc> = row.get("called_at");
        let deadline: chrono::DateTime<chrono::Utc> = row.get("deadline");

        let was_reverted = revert_landed(
            con,
            &article,
            rev_id,
            called_at.timestamp_millis(),
            deadline.timestamp_millis(),
        )
        .await;

        let correct = call_is_correct(verdict, was_reverted);
        let new_elo = elo_update(f64::from(elo), correct);

        let mut tx = pool.begin().await.context("begin settle tx")?;
        sqlx::query("UPDATE calls SET outcome = $1, settled_at = now() WHERE id = $2")
            .bind(was_reverted)
            .bind(call_id)
            .execute(&mut *tx)
            .await
            .context("updating call")?;
        sqlx::query("UPDATE players SET elo = $1 WHERE id = $2")
            .bind(new_elo as f32)
            .bind(row.get::<Option<i64>, _>("player_id"))
            .execute(&mut *tx)
            .await
            .context("updating elo")?;
        tx.commit().await.context("commit settle tx")?;

        let _: Result<i64, _> = redis::cmd("ZREM")
            .arg(keys::SETTLE_QUEUE)
            .arg(&call_id_str)
            .query_async(con)
            .await;

        settled += 1;
        tracing::info!(
            call_id,
            %article,
            verdict,
            was_reverted,
            correct,
            elo_before = elo,
            elo_after = new_elo as f32,
            "PATROL CALL SETTLED"
        );
    }
    Ok(settled)
}

/// Did a strong revert land on this article inside the call's window?
///
/// Prefers an exact revision match; falls back to "any strong revert on this
/// article in the window", because plenty of real revert comments name the
/// editor but not the revision (see revert.rs).
async fn revert_landed(
    con: &mut ConnectionManager,
    article: &str,
    rev_id: i64,
    from_ms: i64,
    to_ms: i64,
) -> bool {
    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(reverts_key(article))
        .arg(from_ms)
        .arg(to_ms)
        .query_async(con)
        .await
        .unwrap_or_default();

    let exact = format!("rev:{rev_id}");
    members.iter().any(|m| *m == exact) || members.iter().any(|m| m.starts_with("any:"))
}

/// Settle Surge bets when an article confirms (PLAN.md Phase 5).
///
/// Paid only when the bet was placed BEFORE the detection and before expiry —
/// "paid only if you called it before confirmation" is the whole point.
pub async fn settle_surge_on_confirmation(
    pool: &PgPool,
    article: &str,
    detected_at: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    let mut tx = pool.begin().await.context("begin surge tx")?;

    let rows = sqlx::query(
        "UPDATE surge_bets
            SET won = true, settled_at = now()
          WHERE article = $1
            AND won IS NULL
            AND placed_at < $2
            AND expires_at > $2
        RETURNING player_id, stake",
    )
    .bind(article)
    .bind(detected_at)
    .fetch_all(&mut *tx)
    .await
    .context("settling surge bets")?;

    for row in &rows {
        let stake: i32 = row.get("stake");
        // Stake was deducted at placement; a win returns it doubled.
        sqlx::query("UPDATE players SET points = points + $1 WHERE id = $2")
            .bind(i64::from(stake) * 2)
            .bind(row.get::<Option<i64>, _>("player_id"))
            .execute(&mut *tx)
            .await
            .context("paying surge win")?;
    }
    tx.commit().await.context("commit surge tx")?;
    Ok(rows.len() as u64)
}

/// Zero out bets that expired without a confirmation.
pub async fn sweep_expired_surge(pool: &PgPool) -> Result<u64> {
    let res = sqlx::query(
        "UPDATE surge_bets SET won = false, settled_at = now()
          WHERE won IS NULL AND expires_at <= now()",
    )
    .execute(pool)
    .await
    .context("sweeping expired surge bets")?;
    Ok(res.rows_affected())
}

/// Credit the First Responder who flagged this article before confirmation
/// (ARCHITECTURE.md §3.2 `events.first_flagger`).
pub async fn credit_first_flagger(
    con: &mut ConnectionManager,
    pool: &PgPool,
    article: &str,
    event_id: i64,
) -> Result<Option<i64>> {
    let key = format!("pulse:flag:{article}");
    let player_id: Option<i64> = redis::cmd("GET").arg(&key).query_async(con).await.ok().flatten();
    let Some(player_id) = player_id else {
        return Ok(None);
    };
    sqlx::query("UPDATE events SET first_flagger = $1 WHERE id = $2")
        .bind(player_id)
        .bind(event_id)
        .execute(pool)
        .await
        .context("crediting first flagger")?;
    let _: Result<i64, _> = redis::cmd("DEL").arg(&key).query_async(con).await;
    Ok(Some(player_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_player_is_an_underdog_against_the_house() {
        // Default rating is 1000 (§3.2), house is 1200, so expected < 0.5.
        let e = expected_score(1000.0);
        assert!(e < 0.5, "got {e}");
        assert!((e - 0.2402).abs() < 0.001, "got {e}");
    }

    #[test]
    fn equal_rating_gives_even_odds() {
        assert!((expected_score(HOUSE_ELO) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn a_correct_call_raises_rating_and_a_wrong_one_lowers_it() {
        let up = elo_update(1000.0, true);
        let down = elo_update(1000.0, false);
        assert!(up > 1000.0, "got {up}");
        assert!(down < 1000.0, "got {down}");
        // An underdog gains more than they lose.
        assert!(up - 1000.0 > 1000.0 - down);
    }

    #[test]
    fn rating_change_never_exceeds_k() {
        for elo in [400.0, 1000.0, 1200.0, 2400.0] {
            assert!((elo_update(elo, true) - elo).abs() <= ELO_K + 1e-9);
            assert!((elo_update(elo, false) - elo).abs() <= ELO_K + 1e-9);
        }
    }

    #[test]
    fn correctness_is_verdict_matching_reality() {
        // Called vandalism, got reverted -> right.
        assert!(call_is_correct(true, true));
        // Called legit, was not reverted -> right.
        assert!(call_is_correct(false, false));
        // Called vandalism, nobody reverted -> wrong.
        assert!(!call_is_correct(true, false));
        // Called legit, but it was reverted -> wrong.
        assert!(!call_is_correct(false, true));
    }
}
