//! Shared model + config for every Pulse service.
//!
//! `RcEvent` mirrors the Wikimedia `recentchange` SSE payload. It is
//! deliberately tolerant: the stream carries ~300 wikis' worth of
//! variation and unknown/absent fields must never abort ingest.

pub mod config;
pub mod db;
pub mod keys;

use serde::{Deserialize, Serialize};

/// One event off the `recentchange` stream.
///
/// Every field the detector or the games rely on is modelled explicitly;
/// anything else is preserved in nothing — we keep the raw JSON line
/// separately, so this struct only needs the fields we compute on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RcEvent {
    /// Stream envelope: carries the resume id and the event timestamp.
    pub meta: Meta,

    /// MediaWiki's own recentchange id. Absent on some `log` events.
    #[serde(default)]
    pub id: Option<i64>,

    /// `edit` | `new` | `categorize` | `log` | ...
    #[serde(rename = "type")]
    pub kind: String,

    /// 0 = main/article namespace. Absent on a few event types.
    #[serde(default)]
    pub namespace: Option<i64>,

    pub title: String,

    #[serde(default)]
    pub title_url: Option<String>,

    #[serde(default)]
    pub comment: String,

    #[serde(default)]
    pub parsedcomment: Option<String>,

    /// Unix seconds, as MediaWiki reports it.
    #[serde(default)]
    pub timestamp: Option<i64>,

    #[serde(default)]
    pub user: String,

    #[serde(default)]
    pub bot: bool,

    #[serde(default)]
    pub minor: bool,

    #[serde(default)]
    pub patrolled: Option<bool>,

    /// Byte counts before/after. Either side can be null on page creation.
    #[serde(default)]
    pub length: Option<Length>,

    /// Revision ids before/after — `Undid revision N` refers to these.
    #[serde(default)]
    pub revision: Option<Revision>,

    /// `enwiki`, `hiwiki`, `commonswiki`, ...
    #[serde(default)]
    pub wiki: String,

    #[serde(default)]
    pub server_name: Option<String>,

    #[serde(default)]
    pub server_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    /// The SSE event id — what `Last-Event-ID` resumes from.
    #[serde(default)]
    pub id: Option<String>,
    /// ISO-8601 event time assigned by the event platform.
    #[serde(default)]
    pub dt: Option<String>,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Length {
    #[serde(default)]
    pub old: Option<i64>,
    #[serde(default)]
    pub new: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Revision {
    #[serde(default)]
    pub old: Option<i64>,
    #[serde(default)]
    pub new: Option<i64>,
}

impl RcEvent {
    /// Canonical article key used for every window, counter and receipt:
    /// `"{wiki}:{title}"`.
    pub fn article_key(&self) -> String {
        format!("{}:{}", self.wiki, self.title)
    }

    /// Byte delta of this edit. `None` when the stream didn't report both sides.
    pub fn delta(&self) -> Option<i64> {
        let l = self.length?;
        Some(l.new.unwrap_or(0) - l.old.unwrap_or(0))
    }

    /// True for the event types that carry article activity we care about.
    /// `log` events (blocks, moves, uploads) are dropped at the ingest edge.
    pub fn is_interesting(&self) -> bool {
        matches!(self.kind.as_str(), "edit" | "new" | "categorize")
    }

    /// Main/article namespace. Talk pages and project pages burst for
    /// reasons that are not news.
    pub fn is_main_namespace(&self) -> bool {
        self.namespace == Some(0)
    }

    /// Anonymous edits self-identify: MediaWiki puts the IP in `user`.
    pub fn is_anon(&self) -> bool {
        is_anon_user(&self.user)
    }
}

/// True when a username is really an IP address — MediaWiki's marker for an
/// anonymous editor. Gate 2 counts registered editors, so this is shared logic.
pub fn is_anon_user(user: &str) -> bool {
    user.parse::<std::net::IpAddr>().is_ok()
}

/// Typed events the detector emits. `Unclassified` is the honest default —
/// a wrong `Death` label is worse than no label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Death,
    Disaster,
    Sports,
    Political,
    Unclassified,
}

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::Death => "death",
            EventKind::Disaster => "disaster",
            EventKind::Sports => "sports",
            EventKind::Political => "political",
            EventKind::Unclassified => "unclassified",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "meta": {"id":"abc-123","dt":"2026-08-09T05:00:00Z","domain":"en.wikipedia.org"},
        "id": 1234567,
        "type": "edit",
        "namespace": 0,
        "title": "Test Article",
        "comment": "Undid revision 999 by [[Special:Contributions/1.2.3.4|1.2.3.4]]",
        "timestamp": 1786000000,
        "user": "SomeEditor",
        "bot": false,
        "minor": false,
        "length": {"old": 100, "new": 60},
        "revision": {"old": 999, "new": 1000},
        "wiki": "enwiki"
    }"#;

    #[test]
    fn parses_a_real_shaped_event() {
        let ev: RcEvent = serde_json::from_str(SAMPLE).unwrap();
        assert_eq!(ev.article_key(), "enwiki:Test Article");
        assert_eq!(ev.delta(), Some(-40));
        assert!(ev.is_interesting());
        assert!(ev.is_main_namespace());
        assert!(!ev.is_anon());
        assert_eq!(ev.meta.id.as_deref(), Some("abc-123"));
    }

    #[test]
    fn tolerates_a_sparse_event() {
        // Minimum the stream can hand us: missing length, revision, comment.
        let ev: RcEvent = serde_json::from_str(
            r#"{"meta":{},"type":"categorize","title":"Category:X","wiki":"dewiki"}"#,
        )
        .unwrap();
        assert_eq!(ev.delta(), None);
        assert!(ev.is_interesting());
        assert!(!ev.is_main_namespace());
    }

    #[test]
    fn detects_anonymous_editors_by_ip_username() {
        let ev: RcEvent = serde_json::from_str(
            r#"{"meta":{},"type":"edit","title":"X","wiki":"enwiki","user":"203.0.113.9"}"#,
        )
        .unwrap();
        assert!(ev.is_anon());
    }

    #[test]
    fn anon_detection_covers_ipv6_and_rejects_names() {
        assert!(is_anon_user("203.0.113.9"));
        assert!(is_anon_user("2001:db8::1"));
        assert!(!is_anon_user("SomeEditor"));
        assert!(!is_anon_user(""));
    }
}
