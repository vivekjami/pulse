//! Classification — ARCHITECTURE.md §4.
//!
//! The stream annotates itself, so there is no ML here. Two evidence sources,
//! in confidence order:
//!
//! 1. `categorize` events — a page being added to `2026 deaths` is the wiki's
//!    own editors asserting the fact. Strongest signal available.
//! 2. Edit-comment keywords, English + Hindi to start (§4).
//!
//! "Precision over coverage — a wrong `death` label is worse than no label."
//! Anything unrecognised stays `Unclassified`, which the receipts page shows
//! honestly rather than guessing.

use common::EventKind;

/// Category-title patterns, checked lowercased. Ordered by specificity.
fn category_kind(category: &str) -> Option<EventKind> {
    let c = category.to_lowercase();

    // "2026 deaths", "Category:2026 deaths", "deaths in August 2026"
    if c.contains("deaths") || c.contains("deceased") || c.ends_with(" obituaries") {
        return Some(EventKind::Death);
    }
    if c.contains("disasters")
        || c.contains("earthquakes")
        || c.contains("floods")
        || c.contains("wildfires")
        || c.contains("plane crashes")
        || c.contains("cyclones")
        || c.contains("hurricanes")
    {
        return Some(EventKind::Disaster);
    }
    if c.contains("elections")
        || c.contains("prime ministers")
        || c.contains("heads of state")
        || c.contains("political scandals")
        || c.contains("coups")
    {
        return Some(EventKind::Political);
    }
    if c.contains("football")
        || c.contains("cricket")
        || c.contains("olympic")
        || c.contains("matches")
        || c.contains("tournaments")
        || c.contains("championships")
        || c.contains("seasons")
    {
        return Some(EventKind::Sports);
    }
    None
}

/// Comment keyword lists per type (§4), English + Hindi.
/// Kept deliberately narrow — a generic word like "won" appears constantly in
/// maintenance edits and would poison precision.
const DEATH_WORDS: &[&str] = &[
    "died", "death date", "passed away", "date of death", "obituary", "dies aged",
    // hi: demise / death / passed away
    "निधन", "मृत्यु", "मौत",
];
const DISASTER_WORDS: &[&str] = &[
    "earthquake", "magnitude", "death toll", "casualties", "tsunami", "derailment",
    "plane crash", "wildfire",
    // hi: earthquake / flood / accident / storm
    "भूकंप", "बाढ़", "दुर्घटना", "तूफान",
];
const POLITICAL_WORDS: &[&str] = &[
    "resigned", "resignation", "sworn in", "elected president", "election result",
    "no-confidence", "impeach", "cabinet reshuffle",
    // hi: resignation / election / prime minister
    "इस्तीफा", "चुनाव", "प्रधानमंत्री",
];
const SPORTS_WORDS: &[&str] = &[
    "final score", "full-time", "aggregate", "man of the match", "wickets",
    "goalscorer", "penalty shootout", "clean sheet",
    // hi: match / innings / victory
    "मैच", "पारी",
];

fn comment_kind(comment: &str) -> Option<EventKind> {
    let c = comment.to_lowercase();
    // Order matters only for overlapping vocabularies; death is the most
    // consequential label, so it is checked first and must be the most specific.
    for (words, kind) in [
        (DEATH_WORDS, EventKind::Death),
        (DISASTER_WORDS, EventKind::Disaster),
        (POLITICAL_WORDS, EventKind::Political),
        (SPORTS_WORDS, EventKind::Sports),
    ] {
        if words.iter().any(|w| c.contains(w)) {
            return Some(kind);
        }
    }
    None
}

/// Classify a confirmed burst from the evidence gathered in its window.
///
/// Categories outvote comments: an editor adding a page to `2026 deaths` is a
/// stronger claim than the word "died" appearing in prose.
pub fn classify(categories: &[String], comments: &[String]) -> EventKind {
    for category in categories {
        if let Some(kind) = category_kind(category) {
            return kind;
        }
    }
    for comment in comments {
        if let Some(kind) = comment_kind(comment) {
            return kind;
        }
    }
    EventKind::Unclassified
}

/// Extract the article a `categorize` event refers to.
///
/// A categorize event's `title` is the *category* page; the article that moved
/// is named in the comment, e.g.
/// `[[:Ada Lovelace]] added to category` / `... removed from category`.
/// Only additions are evidence — a removal is the opposite claim.
pub fn categorized_article(comment: &str) -> Option<&str> {
    if !comment.contains("added to category") {
        return None;
    }
    let start = comment.find("[[:")? + 3;
    let rest = &comment[start..];
    let end = rest.find("]]")?;
    let title = &rest[..end];
    if title.is_empty() {
        None
    } else {
        Some(title)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn category_beats_comment_when_both_are_present() {
        // Comment says sports, category says death — the category wins.
        let kind = classify(&s(&["Category:2026 deaths"]), &s(&["final score 3-1"]));
        assert_eq!(kind, EventKind::Death);
    }

    #[test]
    fn recognises_each_category_family() {
        assert_eq!(classify(&s(&["2026 deaths"]), &[]), EventKind::Death);
        assert_eq!(
            classify(&s(&["Earthquakes in Japan"]), &[]),
            EventKind::Disaster
        );
        assert_eq!(
            classify(&s(&["2026 Indian general elections"]), &[]),
            EventKind::Political
        );
        assert_eq!(
            classify(&s(&["2026–27 Premier League seasons"]), &[]),
            EventKind::Sports
        );
    }

    #[test]
    fn falls_back_to_comment_keywords_in_english() {
        assert_eq!(classify(&[], &s(&["updated death date"])), EventKind::Death);
        assert_eq!(
            classify(&[], &s(&["magnitude 7.1 earthquake"])),
            EventKind::Disaster
        );
        assert_eq!(classify(&[], &s(&["PM resigned today"])), EventKind::Political);
        assert_eq!(classify(&[], &s(&["final score 2-0"])), EventKind::Sports);
    }

    #[test]
    fn falls_back_to_comment_keywords_in_hindi() {
        assert_eq!(classify(&[], &s(&["उनका निधन हो गया"])), EventKind::Death);
        assert_eq!(classify(&[], &s(&["भूकंप की तीव्रता"])), EventKind::Disaster);
        assert_eq!(classify(&[], &s(&["इस्तीफा दे दिया"])), EventKind::Political);
    }

    #[test]
    fn unknown_evidence_stays_unclassified_rather_than_guessing() {
        assert_eq!(
            classify(&s(&["Category:Pages with syntax errors"]), &s(&["typo fix"])),
            EventKind::Unclassified
        );
        assert_eq!(classify(&[], &[]), EventKind::Unclassified);
    }

    #[test]
    fn maintenance_vocabulary_does_not_produce_a_death_label() {
        // The precision rule: a wrong "death" is worse than no label.
        for comment in [
            "Reverted 1 edit by Example",
            "adding coordinates",
            "fixing dead link",
            "updated statistics table",
        ] {
            assert_eq!(
                classify(&[], &s(&[comment])),
                EventKind::Unclassified,
                "{comment:?} must not classify"
            );
        }
    }

    #[test]
    fn extracts_the_article_from_a_categorize_addition() {
        assert_eq!(
            categorized_article("[[:Ada Lovelace]] added to category"),
            Some("Ada Lovelace")
        );
    }

    #[test]
    fn ignores_category_removals_and_malformed_comments() {
        assert_eq!(
            categorized_article("[[:Ada Lovelace]] removed from category"),
            None
        );
        assert_eq!(categorized_article("added to category"), None);
        assert_eq!(categorized_article("[[:]] added to category"), None);
        assert_eq!(categorized_article(""), None);
    }
}
