//! The Valkey keyspace, in one place.
//!
//! Every key Pulse touches is built here so the bus, the windows and the
//! resume pointer can never drift between the three services that share them.

/// Bus stream: every interesting raw event, appended by `ingest`.
pub const BUS_EDITS: &str = "pulse:bus:edits";

/// Bus stream: confirmed bursts, appended by `detector`, fanned out by `api`.
pub const BUS_CONFIRMED: &str = "pulse:bus:confirmed";

/// Consumer group the detector reads `BUS_EDITS` with.
pub const GROUP_DETECTOR: &str = "grpdetector";

/// The gapless-ingest pointer: last SSE event id we durably appended.
pub const INGEST_LAST_EVENT_ID: &str = "pulse:ingest:last_event_id";

/// Per-article sliding window of recent edit timestamps.
pub fn window(article: &str) -> String {
    format!("pulse:win:{article}")
}

/// Distinct-editor set for an article within a 10-minute bucket.
pub fn editors(article: &str, bucket10m: i64) -> String {
    format!("pulse:eds:{article}:{bucket10m}")
}

/// Global stream rate counter for a 1-minute bucket — the bot-flood normalizer.
pub fn global_rate(bucket1m: i64) -> String {
    format!("pulse:global:rate:{bucket1m}")
}

/// Per-article EWMA baseline hash.
pub const EWMA: &str = "pulse:ewma";

/// Vandal Patrol settlement queue, scored by deadline.
pub const SETTLE_QUEUE: &str = "pulse:settle";

/// Bucket a unix-millisecond timestamp into 10-minute slots.
pub fn bucket_10m(ts_ms: i64) -> i64 {
    ts_ms / 600_000
}

/// Bucket a unix-millisecond timestamp into 1-minute slots.
pub fn bucket_1m(ts_ms: i64) -> i64 {
    ts_ms / 60_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_namespaced_and_stable() {
        assert_eq!(window("enwiki:Foo"), "pulse:win:enwiki:Foo");
        assert_eq!(editors("enwiki:Foo", 42), "pulse:eds:enwiki:Foo:42");
        assert_eq!(global_rate(7), "pulse:global:rate:7");
    }

    #[test]
    fn buckets_divide_on_the_expected_boundaries() {
        assert_eq!(bucket_1m(59_999), 0);
        assert_eq!(bucket_1m(60_000), 1);
        assert_eq!(bucket_10m(599_999), 0);
        assert_eq!(bucket_10m(600_000), 1);
    }
}
