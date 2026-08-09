//! The gates — ARCHITECTURE.md §4.
//!
//! Pure functions over already-collected numbers, so the thresholds can be
//! reasoned about and tested without Valkey, Postgres or the network. All
//! constants live in `Tunables`, read from env at boot: PLAN.md Phase 2 requires
//! them tunable without a rebuild, because `k1` needs live tuning against real
//! traffic and a rebuild costs minutes.

use common::is_anon_user;

/// Every threshold in the detector, overridable by env var.
#[derive(Debug, Clone, Copy)]
pub struct Tunables {
    /// Gate 1 sensitivity. §4 starts at 8; lower = more permissive.
    pub k1: f64,
    /// Gate 1 floor: an article must sustain this many edits in the window
    /// regardless of how anomalous the ratio looks (§4: ≥6 edits / 5 min).
    pub min_window_edits: f64,
    /// Guards division when an article has no history (§4's ε).
    ///
    /// Not a free parameter: for a cold article the ratio test reduces to
    /// `edits > rate_window_minutes · k1 · ε`, so ε is what decides the
    /// detection point of a previously-quiet page. §4 puts its floor at 6 edits
    /// per 5 minutes, and ε = 0.15 makes the ratio bind at exactly that number
    /// (5 · 8 · 0.15 = 6). Larger values silently raise the real bar — ε = 0.5
    /// demands >20 edits/5min and nothing fires on live traffic.
    pub epsilon: f64,
    /// Per-article EWMA smoothing (§4: α = 0.3, 1-minute buckets).
    pub ewma_alpha: f64,
    /// Global-baseline smoothing. §4 wants a 1-hour baseline; as an EWMA over
    /// 1-minute buckets that is α ≈ 2/(60+1).
    pub global_alpha: f64,
    /// Gate 2: distinct non-bot editors required.
    pub min_editors: usize,
    /// Gate 2: registered (non-anonymous) editors required.
    pub min_registered: usize,
    /// Gate 2: the busiest editor may not exceed this share of the window.
    pub max_top_share: f64,
    /// Sliding window length in seconds (§3.1 trims to 15 min; §4 rates use 5).
    pub window_secs: i64,
    pub rate_window_secs: i64,
    /// Suppress re-confirming the same article for this long.
    pub cooldown_secs: i64,
}

impl Default for Tunables {
    fn default() -> Self {
        Self {
            k1: 8.0,
            min_window_edits: 6.0,
            epsilon: 0.15,
            ewma_alpha: 0.3,
            global_alpha: 2.0 / 61.0,
            min_editors: 5,
            min_registered: 2,
            max_top_share: 0.5,
            window_secs: 900,
            rate_window_secs: 300,
            cooldown_secs: 21_600,
        }
    }
}

impl Tunables {
    /// Read overrides from env. Anything unset keeps the §4 default.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            k1: common::config::number("PULSE_K1", d.k1),
            min_window_edits: common::config::number("PULSE_MIN_WINDOW_EDITS", d.min_window_edits),
            epsilon: common::config::number("PULSE_EPSILON", d.epsilon),
            ewma_alpha: common::config::number("PULSE_EWMA_ALPHA", d.ewma_alpha),
            global_alpha: common::config::number("PULSE_GLOBAL_ALPHA", d.global_alpha),
            min_editors: common::config::number("PULSE_MIN_EDITORS", d.min_editors),
            min_registered: common::config::number("PULSE_MIN_REGISTERED", d.min_registered),
            max_top_share: common::config::number("PULSE_MAX_TOP_SHARE", d.max_top_share),
            window_secs: common::config::number("PULSE_WINDOW_SECS", d.window_secs),
            rate_window_secs: common::config::number("PULSE_RATE_WINDOW_SECS", d.rate_window_secs),
            cooldown_secs: common::config::number("PULSE_COOLDOWN_SECS", d.cooldown_secs),
        }
    }
}

/// Exponentially-weighted moving average step.
pub fn ewma_step(previous: f64, sample: f64, alpha: f64) -> f64 {
    alpha * sample + (1.0 - alpha) * previous
}

/// What Gate 1 saw, kept for the receipt's evidence trail.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gate1 {
    pub fired: bool,
    /// Edits in the rate window.
    pub window_edits: f64,
    /// r_a / max(μ_a, ε) — how anomalous this article is against itself.
    pub anomaly: f64,
    /// k1 · (G / μ_G) — the bar, raised when the whole stream is busy.
    pub threshold: f64,
    /// G / μ_G, surfaced so a rejected burst can be explained.
    pub global_factor: f64,
}

/// §4 Gate 1 — rate anomaly, normalized against the global stream rate.
///
/// The `G/μ_G` term is the bot-flood normalizer: when the whole stream doubles,
/// every per-article threshold doubles with it, so a maintenance sweep across
/// thousands of pages does not light up the whole board at once.
pub fn gate1(
    window_edits: f64,
    article_ewma: f64,
    global_rate: f64,
    global_baseline: f64,
    t: &Tunables,
) -> Gate1 {
    // Until the global baseline has warmed up, treat the stream as nominal
    // rather than inventing a ratio from a near-zero denominator.
    let global_factor = if global_baseline > 0.0 && global_rate > 0.0 {
        global_rate / global_baseline
    } else {
        1.0
    };

    // Per-minute rate over the window, compared against the per-minute EWMA.
    let per_min = window_edits / (t.rate_window_secs as f64 / 60.0);
    let anomaly = per_min / article_ewma.max(t.epsilon);
    let threshold = t.k1 * global_factor;

    let fired = anomaly > threshold && window_edits >= t.min_window_edits;
    Gate1 {
        fired,
        window_edits,
        anomaly,
        threshold,
        global_factor,
    }
}

/// Per-editor edit counts inside the window, bots already excluded.
#[derive(Debug, Clone, Default)]
pub struct EditorTally {
    pub counts: Vec<(String, u32)>,
}

impl EditorTally {
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, u32)>) -> Self {
        Self {
            counts: pairs.into_iter().collect(),
        }
    }

    pub fn distinct(&self) -> usize {
        self.counts.len()
    }

    /// Registered = username is not an IP address.
    pub fn registered(&self) -> usize {
        self.counts
            .iter()
            .filter(|(user, _)| !is_anon_user(user))
            .count()
    }

    pub fn total_edits(&self) -> u32 {
        self.counts.iter().map(|(_, n)| *n).sum()
    }

    /// Share of window edits made by the single busiest editor.
    /// Zero edits means no dominance, not division by zero.
    pub fn top_share(&self) -> f64 {
        let total = self.total_edits();
        if total == 0 {
            return 0.0;
        }
        let top = self.counts.iter().map(|(_, n)| *n).max().unwrap_or(0);
        f64::from(top) / f64::from(total)
    }
}

/// What Gate 2 saw.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gate2 {
    pub fired: bool,
    pub distinct_editors: usize,
    pub registered_editors: usize,
    pub top_share: f64,
}

/// §4 Gate 2 — editor diversity.
///
/// 40 edits by one user is a rewrite. 40 edits by 25 distinct humans is news.
/// A rejection here with two dominant editors and reverts is an edit-war
/// candidate, which is why the gates feed the conflict radar too (Phase 4).
pub fn gate2(tally: &EditorTally, t: &Tunables) -> Gate2 {
    let distinct_editors = tally.distinct();
    let registered_editors = tally.registered();
    let top_share = tally.top_share();

    let fired = distinct_editors >= t.min_editors
        && registered_editors >= t.min_registered
        && top_share <= t.max_top_share;

    Gate2 {
        fired,
        distinct_editors,
        registered_editors,
        top_share,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tun() -> Tunables {
        Tunables::default()
    }

    // ── Gate 1 ────────────────────────────────────────────────────────────

    #[test]
    fn quiet_article_erupting_fires_gate1() {
        // 30 edits in 5 min = 6/min against a 0.5/min baseline = 12x anomaly,
        // versus a threshold of 8 on a nominal stream.
        let g = gate1(30.0, 0.5, 100.0, 100.0, &tun());
        assert!(g.fired, "{g:?}");
        assert_eq!(g.global_factor, 1.0);
        assert!((g.anomaly - 12.0).abs() < 1e-9);
        assert!((g.threshold - 8.0).abs() < 1e-9);
    }

    #[test]
    fn busy_article_at_its_normal_rate_does_not_fire() {
        // A page that always gets 6 edits/min is not news when it does so again.
        let g = gate1(30.0, 6.0, 100.0, 100.0, &tun());
        assert!(!g.fired, "{g:?}");
    }

    #[test]
    fn global_flood_raises_the_bar_and_suppresses_the_same_burst() {
        // Identical article behaviour; the whole stream has doubled.
        let calm = gate1(30.0, 0.5, 100.0, 100.0, &tun());
        let flood = gate1(30.0, 0.5, 300.0, 100.0, &tun());
        assert!(calm.fired);
        assert!(!flood.fired, "bot flood must not trigger everything: {flood:?}");
        assert!((flood.threshold - 24.0).abs() < 1e-9);
    }

    #[test]
    fn the_defaults_put_the_ratio_and_the_floor_at_the_same_point() {
        // ε is chosen so the two Gate-1 conditions agree rather than one
        // silently dominating: a cold article clears the ratio at
        // rate_window_minutes · k1 · ε = 5 · 8 · 0.15 = 6 edits, which is
        // exactly §4's "≥6 edits / 5 min" floor. Detection therefore begins at
        // the number the spec states, instead of the >20 that ε = 0.5 implied.
        let t = tun();
        assert!((5.0 * t.k1 * t.epsilon - t.min_window_edits).abs() < 1e-9);

        // Below the stated floor: rejected.
        assert!(!gate1(5.0, 0.0, 100.0, 100.0, &t).fired);
        // At the floor the ratio is exactly at the bar, and `>` is strict.
        let at = gate1(6.0, 0.0, 100.0, 100.0, &t);
        assert!((at.anomaly - at.threshold).abs() < 1e-9, "{at:?}");
        assert!(!at.fired, "strictly greater, so the boundary itself does not fire");
        // One edit past it: detected.
        assert!(gate1(7.0, 0.0, 100.0, 100.0, &t).fired);
    }

    #[test]
    fn the_edit_floor_binds_once_k1_or_epsilon_are_tuned_down() {
        // The floor is not dead code — it is the safety net for the live tuning
        // pass PLAN.md budgets, where k1 comes down until junk stops passing.
        // With a small ε a 5-edit window looks wildly anomalous; the floor is
        // then the only thing rejecting it.
        let t = Tunables {
            epsilon: 0.02,
            ..Tunables::default()
        };
        let g = gate1(5.0, 0.0, 100.0, 100.0, &t);
        assert!(g.anomaly > g.threshold, "ratio should clear the bar: {g:?}");
        assert!(!g.fired, "the ≥6 edit floor must reject it: {g:?}");

        // One more edit and it qualifies.
        assert!(gate1(6.0, 0.0, 100.0, 100.0, &t).fired);
    }

    #[test]
    fn epsilon_guards_a_brand_new_article_from_dividing_by_zero() {
        let g = gate1(30.0, 0.0, 100.0, 100.0, &tun());
        assert!(g.anomaly.is_finite());
        assert!(g.fired);
    }

    #[test]
    fn cold_global_baseline_is_treated_as_nominal() {
        // Before warm-up we must not manufacture a ratio from a zero baseline.
        let g = gate1(30.0, 0.5, 0.0, 0.0, &tun());
        assert_eq!(g.global_factor, 1.0);
        assert!(g.anomaly.is_finite());
    }

    // ── Gate 2 ────────────────────────────────────────────────────────────

    fn tally(pairs: &[(&str, u32)]) -> EditorTally {
        EditorTally::from_pairs(pairs.iter().map(|(u, n)| ((*u).to_string(), *n)))
    }

    #[test]
    fn diverse_crowd_of_humans_fires_gate2() {
        let t = tally(&[
            ("Ann", 2),
            ("Ben", 2),
            ("Cal", 2),
            ("Dee", 1),
            ("Eve", 1),
            ("203.0.113.9", 1),
        ]);
        let g = gate2(&t, &tun());
        assert!(g.fired, "{g:?}");
        assert_eq!(g.distinct_editors, 6);
        assert_eq!(g.registered_editors, 5);
    }

    #[test]
    fn single_author_rewrite_is_rejected() {
        let g = gate2(&tally(&[("Ann", 40)]), &tun());
        assert!(!g.fired);
        assert_eq!(g.top_share, 1.0);
    }

    #[test]
    fn two_person_edit_war_is_rejected_and_routes_to_the_radar() {
        // This is the shape Phase 4's conflict radar wants, not a news burst.
        let g = gate2(&tally(&[("Ann", 10), ("Ben", 9)]), &tun());
        assert!(!g.fired, "{g:?}");
        assert_eq!(g.distinct_editors, 2);
    }

    #[test]
    fn all_anonymous_crowd_fails_the_registered_floor() {
        let g = gate2(
            &tally(&[
                ("203.0.113.1", 1),
                ("203.0.113.2", 1),
                ("203.0.113.3", 1),
                ("203.0.113.4", 1),
                ("203.0.113.5", 1),
            ]),
            &tun(),
        );
        assert!(!g.fired);
        assert_eq!(g.distinct_editors, 5);
        assert_eq!(g.registered_editors, 0);
    }

    #[test]
    fn dominant_editor_in_a_crowd_fails_the_top_share_check() {
        // Six editors, but one made 20 of 25 edits — a rewrite with bystanders.
        let g = gate2(
            &tally(&[
                ("Ann", 20),
                ("Ben", 1),
                ("Cal", 1),
                ("Dee", 1),
                ("Eve", 1),
                ("Fay", 1),
            ]),
            &tun(),
        );
        assert!(!g.fired, "{g:?}");
        assert!(g.top_share > 0.5);
    }

    #[test]
    fn boundaries_are_inclusive_exactly_as_specified() {
        // Exactly 5 distinct, exactly 2 registered, exactly 0.50 top share.
        let t = tally(&[
            ("Ann", 5),
            ("Ben", 2),
            ("203.0.113.1", 1),
            ("203.0.113.2", 1),
            ("203.0.113.3", 1),
        ]);
        let g = gate2(&t, &tun());
        assert_eq!(g.distinct_editors, 5);
        assert_eq!(g.registered_editors, 2);
        assert!((g.top_share - 0.5).abs() < 1e-9);
        assert!(g.fired, "≥5, ≥2 and ≤0.5 must all pass at the boundary: {g:?}");
    }

    #[test]
    fn empty_tally_is_inert_not_a_panic() {
        let g = gate2(&EditorTally::default(), &tun());
        assert!(!g.fired);
        assert_eq!(g.top_share, 0.0);
    }

    // ── EWMA ──────────────────────────────────────────────────────────────

    #[test]
    fn ewma_converges_toward_the_sample() {
        let mut v = 0.0;
        for _ in 0..50 {
            v = ewma_step(v, 10.0, 0.3);
        }
        assert!((v - 10.0).abs() < 0.01, "got {v}");
    }

    #[test]
    fn ewma_weights_the_newest_sample_by_alpha() {
        assert!((ewma_step(0.0, 10.0, 0.3) - 3.0).abs() < 1e-9);
    }
}
