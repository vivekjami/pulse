//! Vandal Patrol eligibility — PLAN.md Phase 3.
//!
//! "Enough to keep the queue interesting": non-bot main-namespace edits that are
//! either a suspiciously-sized change or an anonymous deletion. The queue is a
//! capped Valkey list the api serves from, so the browser never touches the bus.
//!
//! Deliberately NOT a vandalism classifier — the player is the classifier, and
//! the revert stream is the ground truth. This filter only has to avoid boring
//! the player.

use anyhow::{Context, Result};
use common::RcEvent;
use redis::aio::ConnectionManager;

/// The queue the api reads. Capped: a stale candidate is worse than none.
pub const QUEUE: &str = "pulse:patrol:queue";
const QUEUE_MAX: isize = 120;

/// Byte-delta band that tends to contain both vandalism and legitimate edits —
/// large enough to be visible, small enough not to be a rewrite.
const SUSPICIOUS_MIN: i64 = 40;
const SUSPICIOUS_MAX: i64 = 4_000;

/// Is this edit worth putting in front of a player?
pub fn is_eligible(ev: &RcEvent) -> bool {
    if ev.bot || !ev.is_main_namespace() {
        return false;
    }
    // Needs a revision to link a diff to, and a revision to settle against.
    let Some(rev) = ev.revision.and_then(|r| r.new) else {
        return false;
    };
    if rev <= 0 {
        return false;
    }
    let Some(delta) = ev.delta() else {
        return false;
    };

    // Anonymous removals are the classic vandalism shape.
    if ev.is_anon() && delta < 0 {
        return true;
    }
    // Otherwise: a change big enough to judge from the comment and size alone.
    let magnitude = delta.abs();
    (SUSPICIOUS_MIN..=SUSPICIOUS_MAX).contains(&magnitude)
}

/// Push an eligible edit onto the patrol queue.
pub async fn enqueue(con: &mut ConnectionManager, ev: &RcEvent) -> Result<()> {
    let payload = serde_json::json!({
        "article": ev.article_key(),
        "wiki": ev.wiki,
        "title": ev.title,
        "title_url": ev.title_url,
        "user": ev.user,
        "anon": ev.is_anon(),
        "comment": ev.comment,
        "delta": ev.delta(),
        "rev_id": ev.revision.and_then(|r| r.new),
        "old_rev_id": ev.revision.and_then(|r| r.old),
        "at": ev.meta.dt,
        // Link out to Wikipedia's own diff — PLAN.md Phase 3 is explicit that
        // the MVP must not fetch or render diffs server-side.
        "diff_url": diff_url(ev),
    });

    redis::pipe()
        .atomic()
        .cmd("LPUSH")
        .arg(QUEUE)
        .arg(payload.to_string())
        .ignore()
        .cmd("LTRIM")
        .arg(QUEUE)
        .arg(0)
        .arg(QUEUE_MAX - 1)
        .ignore()
        .query_async::<()>(con)
        .await
        .context("enqueueing patrol candidate")?;
    Ok(())
}

/// Wikipedia's own diff URL for this revision.
fn diff_url(ev: &RcEvent) -> Option<String> {
    let server = ev.server_url.as_deref()?;
    let rev = ev.revision.and_then(|r| r.new)?;
    Some(format!("{server}/w/index.php?diff={rev}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(json: &str) -> RcEvent {
        serde_json::from_str(json).unwrap()
    }

    const ANON_DELETION: &str = r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"203.0.113.9","length":{"old":500,"new":480},"revision":{"old":1,"new":2},"server_url":"https://en.wikipedia.org"}"#;

    #[test]
    fn anonymous_removals_are_eligible_even_when_small() {
        let e = ev(ANON_DELETION);
        assert_eq!(e.delta(), Some(-20));
        assert!(is_eligible(&e), "anon + negative delta is the classic shape");
    }

    #[test]
    fn registered_edits_need_a_suspicious_magnitude() {
        // 20 bytes from a named editor: too small to judge, skip.
        let small = ev(r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"Ann","length":{"old":500,"new":520},"revision":{"old":1,"new":2}}"#);
        assert!(!is_eligible(&small));

        let mid = ev(r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"Ann","length":{"old":500,"new":900},"revision":{"old":1,"new":2}}"#);
        assert!(is_eligible(&mid));

        // A 20k rewrite is not a patrol candidate.
        let huge = ev(r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"Ann","length":{"old":0,"new":20000},"revision":{"old":1,"new":2}}"#);
        assert!(!is_eligible(&huge));
    }

    #[test]
    fn bots_and_non_articles_never_reach_a_player() {
        let bot = ev(r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"Bot","bot":true,"length":{"old":500,"new":900},"revision":{"old":1,"new":2}}"#);
        assert!(!is_eligible(&bot));

        let talk = ev(r#"{"meta":{},"type":"edit","namespace":1,"title":"Talk:X","wiki":"enwiki","user":"Ann","length":{"old":500,"new":900},"revision":{"old":1,"new":2}}"#);
        assert!(!is_eligible(&talk));
    }

    #[test]
    fn an_edit_without_a_revision_cannot_be_settled_so_is_skipped() {
        let no_rev = ev(r#"{"meta":{},"type":"edit","namespace":0,"title":"X","wiki":"enwiki","user":"Ann","length":{"old":500,"new":900}}"#);
        assert!(!is_eligible(&no_rev));
    }

    #[test]
    fn diff_url_points_at_wikipedias_own_diff() {
        let e = ev(ANON_DELETION);
        assert_eq!(
            diff_url(&e).as_deref(),
            Some("https://en.wikipedia.org/w/index.php?diff=2")
        );
    }
}
