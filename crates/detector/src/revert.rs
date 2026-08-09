//! Revert parsing — ARCHITECTURE.md §4, versioned in one module with a test
//! corpus of real comments (PLAN.md Phase 4).
//!
//! **The spec's regexes do not match production.** §4 gives
//! `Undid revision (\d+) by \[\[Special:Contributions/([^|]+)\|`, but the live
//! stream emits:
//!
//! ```text
//! Undid revision [[Special:Diff/1368430003|1368430003]] by [[Special:Contributions/Barçaforlife|Barçaforlife]] (...)
//! ```
//!
//! The revision id is wrapped in a `Special:Diff` link, so the spec pattern
//! matches nothing. Likewise §4's `Special:Contribs/` is the rare spelling —
//! `Special:Contributions/` dominates. Every pattern below was checked against
//! comments captured from our own raw log; §8 called this "comment-format
//! drift" and it is real on day one, not later.
//!
//! Semantics that matter for the radar: in "Reverted edits by A ... to last
//! revision by B", A is the reverted party and B is merely who we restored to.
//! In "Restored revision N by X", X is the *restored-to* party — the reverted
//! editor is not named at all. Treating X as reverted would invert the edge and
//! corrupt the conflict graph, so that form yields a revert with no named party.

use regex::Regex;
use std::sync::LazyLock;

/// How much the comment tells us.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    /// An explicit revert. Settles Vandal Patrol calls and writes an incident.
    Strong,
    /// Revert-ish vocabulary only. Bumps the controversy index, nothing else.
    Weak,
}

/// A parsed revert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revert {
    pub confidence: Confidence,
    /// The revision that was undone, when the comment names it.
    pub rev_id: Option<i64>,
    /// The editor whose work was undone, when the comment names them.
    pub reverted_user: Option<String>,
}

/// `Undid revision [[Special:Diff/N|N]] by [[Special:Contributions/USER|...`
static UNDID_WRAPPED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)undid revision \[\[Special:Diff/(\d+)\|[^\]]*\]\] by \[\[Special:Contrib(?:ution)?s/([^|\]]+)",
    )
    .expect("UNDID_WRAPPED")
});

/// §4's literal form, kept because some wikis/tools still emit a bare id.
static UNDID_BARE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)undid revision (\d+) by \[\[Special:Contrib(?:ution)?s/([^|\]]+)")
        .expect("UNDID_BARE")
});

/// `Reverted [3 ]edits? by [[Special:Contributions/USER|...`, tolerating an
/// interposed wiki-link such as `[[WP:AGF|good faith]]` or `[[Commons:Rollback|Reverted]]`.
static REVERTED_BY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)revert(?:ed|ing)\b[^\[]{0,40}(?:\[\[[^\]]+\]\][^\[]{0,20})?edits?\s+(?:added\s+)?by\s+\[\[Special:Contrib(?:ution)?s/([^|\]]+)",
    )
    .expect("REVERTED_BY")
});

/// `Reverting possible vandalism by [[Special:Contribs/USER` and
/// `Reverting unsourced content added by [[Special:Contributions/USER`.
static REVERTING_PROSE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)revert(?:ing|ed)\b[^\[]{0,60}by\s+\[\[Special:Contrib(?:ution)?s/([^|\]]+)")
        .expect("REVERTING_PROSE")
});

/// Spanish rollback: `Revertida una edición de [[Special:Contributions/USER`.
static REVERTIDA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)revertid[ao][^\[]{0,40}de \[\[Special:Contrib(?:ution)?s/([^|\]]+)")
        .expect("REVERTIDA")
});

/// Japanese undo/rollback: `[[Special:Contributions/USER|…]] … による … 取り消し|巻き戻し`.
static JA_REVERT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[\[Special:Contrib(?:ution)?s/([^|\]]+)\|[^\]]*\]\].*(?:取り消し|巻き戻し|差し戻)")
        .expect("JA_REVERT")
});

/// `Restored revision N by [[Special:Contributions/X` — X is who we restored
/// TO, not who was reverted. A revert happened; the party is unnamed.
static RESTORED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)restored revision (\d+)\b").expect("RESTORED"));

/// §4's weak signal — radar only, never settlement.
static WEAK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\brvv?\b|revert|vandal|巻き戻し|取り消し").expect("WEAK"));

/// Parse a revert out of an edit comment.
pub fn parse(comment: &str) -> Option<Revert> {
    if comment.is_empty() {
        return None;
    }

    // Ordered by confidence. `Restored revision` is checked before the generic
    // "Reverted ... by" patterns so its restored-to user is never mistaken for
    // the reverted party.
    if let Some(c) = RESTORED.captures(comment) {
        return Some(Revert {
            confidence: Confidence::Strong,
            rev_id: c.get(1).and_then(|m| m.as_str().parse().ok()),
            reverted_user: None,
        });
    }
    for re in [&*UNDID_WRAPPED, &*UNDID_BARE] {
        if let Some(c) = re.captures(comment) {
            return Some(Revert {
                confidence: Confidence::Strong,
                rev_id: c.get(1).and_then(|m| m.as_str().parse().ok()),
                reverted_user: c.get(2).map(|m| m.as_str().to_string()),
            });
        }
    }
    for re in [&*REVERTED_BY, &*REVERTIDA, &*JA_REVERT, &*REVERTING_PROSE] {
        if let Some(c) = re.captures(comment) {
            return Some(Revert {
                confidence: Confidence::Strong,
                rev_id: None,
                reverted_user: c.get(1).map(|m| m.as_str().to_string()),
            });
        }
    }
    if WEAK.is_match(comment) {
        return Some(Revert {
            confidence: Confidence::Weak,
            rev_id: None,
            reverted_user: None,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured from our own `raw/events.jsonl.gz` — not invented.
    /// §8's "comment-format drift" mitigation is this list.
    const CORPUS: &[&str] = &[
        "Undid revision [[Special:Diff/1368430003|1368430003]] by [[Special:Contributions/Barçaforlife|Barçaforlife]] ([[User talk:Barçaforlife|talk]])",
        "Reverted edit by [[Special:Contributions/~2026-43649-56|~2026-43649-56]] ([[User talk:~2026-43649-56|talk]]) to last revision by [[User:Sotiale|Sotiale]]",
        "Undid revision [[Special:Diff/1368482752|1368482752]] by [[Special:Contributions/Sirfurboy|Sirfurboy]] ([[User talk:Sirfurboy|talk]]) Actually, no.",
        "Reverted edits by [[Special:Contributions/Boomboom6837|Boomboom6837]] ([[User talk:Boomboom6837|talk]]): disruptive edits ([[WP:HG|HG]]) (3.4.14)",
        "Reverted 1 edit by [[Special:Contributions/~2026-43823-84|~2026-43823-84]] ([[User talk:~2026-43823-84|talk]]): [[WP:SIMPLEFLYING]]",
        "[[WP:CVPI|Interceptor]]: Reverting unsourced content added by [[Special:Contributions/Beitmerryprof|Beitmerryprof]]",
        "Reverted 1 edit by [[Special:Contributions/Hairnorth|Hairnorth]] ([[User talk:Hairnorth|talk]]): Date change without any sort of source",
        "Reverted [[WP:AGF|good faith]] edits by [[Special:Contributions/~2026-40270-91|~2026-40270-91]] ([[User talk:~2026-40270-91|talk]]): Incorrect comma",
        "Revertida una edición de [[Special:Contributions/~2026-43833-95|~2026-43833-95]] ([[User talk:~2026-43833-95|disc.]]) a la última edición de SeroBOT",
        "Reverted edits by [[Special:Contributions/Ismith400|Ismith400]] ([[User talk:Ismith400|talk]]) to last revision by [[User:MathXplore|MathXplore]]",
        "Reverted 1 edit by [[Special:Contributions/Eight Bit Cat|Eight Bit Cat]] ([[User talk:Eight Bit Cat|talk]]): Not a useful image",
        "Undid revision [[Special:Diff/1368475886|1368475886]] by [[Special:Contributions/~2026-43799-17|~2026-43799-17]] ([[User talk:~2026-43799-17|talk]]) Not needed",
        "Reverted edits by [[Special:Contribs/~2026-42482-20|~2026-42482-20]] ([[User talk:~2026-42482-20|talk]]) to last version by BridgeBoy1980",
        "Reverted 2 edits by [[Special:Contributions/Fence127|Fence127]] ([[User talk:Fence127|talk]]): Focus on improving articles",
        "[[Commons:Rollback|Reverted]] edits by [[Special:Contributions/Zwaao|Zwaao]] ([[User talk:Zwaao|talk]]) to last revision by Wikimedia Commons Welcome",
        "[[Special:Contributions/~2026-43693-74|~2026-43693-74]] ([[User talk:~2026-43693-74|会話]]) による版を 沢庵柚希 による版へ[[H:RV|巻き戻し]]",
        "Reverting possible vandalism by [[Special:Contribs/~2026-43812-32|~2026-43812-32]] to version by PhuzBot. Thanks, [[WP:CBNG|ClueBot NG]]. (4546516) (Bot)",
        "Revertida una edición de [[Special:Contributions/~2026-43630-08|~2026-43630-08]] ([[User talk:~2026-43630-08|disc.]]) a la última edición de Rafstr",
        "[[Special:Contributions/ぽめらにわん|ぽめらにわん]] ([[User talk:ぽめらにわん|会話]]) による ID:110580999 の版を[[H:RV|取り消し]]",
        "Reverted 3 edits by [[Special:Contributions/~2026-43792-72|~2026-43792-72]] ([[User talk:~2026-43792-72|talk]]) to last revision by Indagate",
        "Undid revision [[Special:Diff/1356473108|1356473108]] by [[Special:Contributions/Trhres|Trhres]] ([[User talk:Trhres|talk]])",
        "Undid revision [[Special:Diff/7278794|7278794]] by [[Special:Contributions/Muhamad Izzul Fiqih|Muhamad Izzul Fiqih]] ([[User talk:Muhamad Izzul Fiqih|talk]])",
        "[[Special:Contributions/Naokun|Naokun]] ([[User talk:Naokun|会話]]) による ID:110579535 の版を[[H:RV|取り消し]] 放送日欄は",
        "Reverted edits by [[Special:Contributions/~2026-43827-14|~2026-43827-14]] ([[User talk:~2026-43827-14|talk]]): not adhering to [[WP:MOS|manual of style]] ([[WP:HG|HG]]) (3.4.14)",
        "Undid revision [[Special:Diff/1368476209|1368476209]] by [[Special:Contributions/Shreya nagre|Shreya nagre]] ([[User talk:Shreya nagre|talk]])",
        "Undid revision [[Special:Diff/1368451775|1368451775]] by [[Special:Contributions/Hughpugh2|Hughpugh2]] ([[User talk:Hughpugh2|talk]])unsourced",
        "Reverted edits by [[Special:Contributions/~2026-43687-70|~2026-43687-70]] ([[User talk:~2026-43687-70|talk]]): [[WP:SANDBOX|editing tests]] ([[WP:HG|HG]]) (3.4.14)",
    ];

    /// Real comments that carry revert vocabulary but name nobody.
    const WEAK_ONLY: &[&str] = &[
        "/* top */ Revert; contains non-American English spelling(s)",
        "Revert; contains non-American English spelling(s)",
        "/* Most appearances */ Vandalism removed",
        "Please do not vandalize Wikimedia Commons.",
        "/* Vandalism */ new section",
    ];

    /// Real comments with no revert semantics at all.
    const NOT_REVERTS: &[&str] = &[
        "/* Khmer */",
        "adding coordinates",
        "updated statistics table",
        "fixing dead link",
        "/* wbsetclaim-create:2||1 */ [[Property:P1716]]: [[Q11900331]]",
        "",
    ];

    #[test]
    fn every_corpus_comment_parses_as_a_strong_revert() {
        for c in CORPUS {
            let r = parse(c).unwrap_or_else(|| panic!("no parse: {c}"));
            assert_eq!(
                r.confidence,
                Confidence::Strong,
                "should be strong: {c}\ngot {r:?}"
            );
        }
    }

    #[test]
    fn the_specs_own_pattern_would_have_missed_the_real_format() {
        // Documents WHY the wrapped pattern exists. §4's bare-id regex finds
        // nothing in the format the stream actually emits.
        let real = CORPUS[0];
        assert!(UNDID_BARE.captures(real).is_none(), "spec pattern matched?");
        assert!(UNDID_WRAPPED.captures(real).is_some());
    }

    #[test]
    fn extracts_rev_id_and_reverted_user_from_an_undo() {
        let r = parse(CORPUS[0]).unwrap();
        assert_eq!(r.rev_id, Some(1368430003));
        assert_eq!(r.reverted_user.as_deref(), Some("Barçaforlife"));
    }

    #[test]
    fn names_the_reverted_party_not_the_restored_to_party() {
        // "Reverted edits by A ... to last revision by B" — A is reverted.
        let r = parse(CORPUS[9]).unwrap();
        assert_eq!(r.reverted_user.as_deref(), Some("Ismith400"));
        assert_ne!(r.reverted_user.as_deref(), Some("MathXplore"));
    }

    #[test]
    fn restored_revision_names_nobody_as_reverted() {
        // X here is who we restored TO. Claiming X was reverted would invert
        // the conflict edge.
        let r = parse(
            "Restored revision 1364274322 by [[Special:Contributions/TuxbietheFixer|TuxbietheFixer]] ([[User talk:TuxbietheFixer|talk]]): Rvv",
        )
        .unwrap();
        assert_eq!(r.confidence, Confidence::Strong);
        assert_eq!(r.rev_id, Some(1364274322));
        assert_eq!(r.reverted_user, None, "must not name the restored-to user");
    }

    #[test]
    fn handles_spanish_and_japanese_rollbacks() {
        let es = parse(CORPUS[8]).unwrap();
        assert_eq!(es.confidence, Confidence::Strong);
        assert_eq!(es.reverted_user.as_deref(), Some("~2026-43833-95"));

        let ja = parse(CORPUS[18]).unwrap();
        assert_eq!(ja.confidence, Confidence::Strong);
        assert_eq!(ja.reverted_user.as_deref(), Some("ぽめらにわん"));
    }

    #[test]
    fn revert_vocabulary_without_a_named_party_is_weak_only() {
        for c in WEAK_ONLY {
            let r = parse(c).unwrap_or_else(|| panic!("no parse: {c}"));
            assert_eq!(r.confidence, Confidence::Weak, "{c}");
            assert_eq!(r.reverted_user, None, "{c}");
        }
    }

    #[test]
    fn ordinary_edits_are_not_reverts() {
        for c in NOT_REVERTS {
            assert!(parse(c).is_none(), "should not parse: {c:?}");
        }
    }

    #[test]
    fn corpus_is_large_enough_to_be_a_regression_net() {
        // PLAN.md Phase 4 asks for ~30 real comments.
        assert!(CORPUS.len() + WEAK_ONLY.len() >= 30);
    }
}
