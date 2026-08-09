//! Configuration, read from OS env at startup.
//!
//! Zerops injects every value as a real env var — there is no `.env` file in
//! a deployed container, and creating one would shadow the platform's vars.
//! Local development uses `docker compose` + a shell-sourced `.env`.

use std::env;

/// Wikimedia's public firehose. No auth, no key.
pub const DEFAULT_STREAM_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

/// Read a required env var, with a message that names the missing key.
pub fn required(key: &str) -> Result<String, String> {
    env::var(key).map_err(|_| format!("missing required env var {key}"))
}

/// Read an optional env var with a fallback.
pub fn optional(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_string())
}

/// Read a numeric env var, falling back when unset or unparseable.
pub fn number<T: std::str::FromStr>(key: &str, fallback: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// Valkey connection URL. Zerops exposes it as `VALKEY_URL` (wired from
/// `${cache_connectionString}` in zerops.yaml).
pub fn valkey_url() -> Result<String, String> {
    required("VALKEY_URL")
}

/// PostgreSQL connection URL, wired from the `${db_*}` catalog.
pub fn database_url() -> Result<String, String> {
    required("DATABASE_URL")
}

/// Port the HTTP server binds. Must bind `0.0.0.0` — the L7 balancer routes
/// to the container's VXLAN IP, so a loopback bind returns 502.
pub fn port() -> u16 {
    number("PORT", 3000)
}

/// The stream endpoint, overridable so a replay harness can point at a fixture.
pub fn stream_url() -> String {
    optional("STREAM_URL", DEFAULT_STREAM_URL)
}

/// Domain suffix the DETECTOR restricts itself to. Empty string = no filter.
///
/// This is a judgment call the architecture doc does not make explicitly, so it
/// is configurable rather than hardcoded. Rationale: §4's classification keys off
/// encyclopedia category conventions ("2026 deaths"), and the Gate 3 design
/// resolves an article to a Wikidata QID and looks for the same entity bursting
/// across ≥3 language wikis — both of which presuppose Wikipedia language
/// editions as the detection target, with Wikidata as the entity resolver rather
/// than a thing to detect on. Left unfiltered, essentially every Gate 1
/// candidate is a single-editor `wikidatawiki:Q…` item under semi-automated
/// editing, which Gate 2 then correctly discards.
///
/// The live wall is deliberately NOT filtered — the README promises "every edit
/// on every Wikimedia wiki", and that stays true.
pub fn detect_domain_suffix() -> String {
    optional("PULSE_DETECT_DOMAIN_SUFFIX", ".wikipedia.org")
}

/// Install the tracing subscriber. `RUST_LOG` controls verbosity;
/// default `info` so Zerops log capture stays useful without being noisy.
pub fn init_tracing(service: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
    tracing::info!(service, "pulse service starting");
}
