# Pulse

**The world's largest collaborative document is a live sensor. Pulse listens to it.**

Wikipedia receives roughly ten edits per second, across ~300 languages, around the clock. When something happens in the real world — a public figure dies, a match ends, a government falls — the relevant articles erupt with edits, often minutes before news sites update. Pulse consumes the full Wikimedia edit firehose in real time, detects those eruptions with a multi-gate statistical detector, classifies them, timestamps them permanently, and turns the stream's own ground truth into a game anyone can play.

## Live

| | |
|---|---|
| **The live wall** | <https://web-2c9c.prg1.zerops.app/> |
| **Conflict radar + watchlist** | on the wall — "Fought over right now" and the Surge watchlist strip |
| **Vandal Patrol** | <https://web-2c9c.prg1.zerops.app/patrol/> |
| **Leaderboard** | <https://web-2c9c.prg1.zerops.app/leaderboard/> |
| **Receipts ledger** | <https://web-2c9c.prg1.zerops.app/events/> |
| **API** | <https://api-2c9c-3000.prg1.zerops.app/> (endpoint index at `/`) |

Built for the Zerops Challenge, Aug 8–9 2026. Data: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) (CC-BY-SA — attribution below). No API keys; the firehose is public.

## What is actually built

This table is the honest version. The design below describes more than one hackathon fits, so everything is marked for what it is.

| Feature | Status |
|---|---|
| Live wall over SSE, 24/7 ingest, gapless resume | **Built**, resume proven live |
| Gate 1 — rate anomaly vs EWMA baseline | **Built** |
| Gate 2 — editor diversity | **Built**, and it is the binding constraint (see *Known gaps*) |
| Classification into typed events | **Built** |
| Receipts ledger + `/events/` page | **Built**, and **currently empty** (see *Known gaps*) |
| Revert parsing from edit comments | **Built** — 27-comment real corpus, 264 incidents recorded live |
| Controversy index + edit-war cycle scan + radar panel | **Built** |
| Vandal Patrol — queue, calls, ELO settlement, leaderboard | **Built**, settlement proven live |
| Call the Surge — bets, watchlist strip, payout on confirmation | **Built**; payout path never fired, because nothing has confirmed |
| First Responder — `POST /v1/flag`, `first_flagger` on the receipt | **API only**, no UI surface |
| Gate 3 — cross-language confirmation via Wikidata | **Not built** (roadmap) |
| Parquet archive + DataFusion SQL endpoint | **Not built** (roadmap) |
| Brigade flags | **Not built** (roadmap) |

## How it works

### 1. The live wall

Every edit on every Wikimedia wiki, streaming into the browser the moment it happens. Wiki badge, title, byte delta, editor, with anonymous edits highlighted. Auto-scroll pauses while the pointer rests on the feed.

The api holds **one** Valkey consumer per stream for the whole process and fans out over a `tokio::sync::broadcast`; each client samples it at ~20 frames/sec. Frames are dropped, never queued, so a slow browser can never apply backpressure to the bus. Confirmed bursts ride a **separate** channel selected `biased`, so a receipt never waits behind the edit sampler.

### 2. Burst detection

Two gates, both tunable at runtime without a rebuild:

- **Gate 1 — rate anomaly.** Per-article edit velocity over a 5-minute window against an exponentially-weighted baseline, normalized by the global stream rate so a bot sweep doesn't light up everything at once. The baseline sample is deliberately **lagged past the rate window** — with α=0.3 the EWMA has a ~3-minute time constant, shorter than the window, so a burst was feeding its own baseline and capping its own anomaly score at 1.4.
- **Gate 2 — editor diversity.** 40 edits by one user is a rewrite or a fight; 40 edits by 25 distinct humans is news. Requires ≥5 distinct editors, ≥2 registered, and no single editor holding >50% of the edits.

Windows are keyed on **event time**, not wall-clock arrival — mixing the two silently mis-buckets every event that arrives late.

Classification comes from the stream's own annotations: category additions like `2026 deaths` outrank comment keywords, giving **death, disaster, sports, political, unclassified**.

Every confirmation writes a permanent row with its evidence trail. Detected-at timestamps cannot be back-dated; that ledger is the proof.

### 3. Conflict radar

Reverts self-identify in edit comments, and the real ones do **not** look like the spec assumed:

```
Undid revision [[Special:Diff/1368430003|1368430003]] by [[Special:Contributions/Barçaforlife|…
```

The parser is written against a 27-entry corpus harvested from our own raw capture, covering English, Spanish and Japanese conventions. `RESTORED` is tested **before** `UNDID`/`REVERTED`, because a "restored to revision by X" comment names the user being restored *to*, who is not the reverted party — checking it last credits the wrong editor on every rollback.

From that stream: a **controversy index** as decayed counters with a 10-minute half-life (so stale articles decay out with no sweep job), and an **edit-war** scan over the last 20 revert edges per article, flagging A↔B ping-pong and A→B→C→A cycles with ≥3 edges inside 30 minutes.

### 4. The game — settled by reality

No invented mechanics. Every score settles against ground truth arriving on the same stream.

| Mode | You do | Ground truth |
|---|---|---|
| **Vandal Patrol** | See a live edit, call *vandalism* or *legit* in 10s | Did a real revert land on that article inside the 10-minute deadline? Auto-settled, ELO K=32 against a fixed 1200 house. |
| **Call the Surge** | Stake points that an article confirm-bursts within the hour | Paid 2× only where `placed_at < detected_at < expires_at` — a bet placed after a burst already confirmed cannot win. |
| **First Responder** | Flag an article pre-confirmation (API only) | If the detector confirms it, the flagger's id lands on the receipt itself. `SET NX`, so the *first* responder wins. |

Diffs are **linked out** to Wikipedia, never proxied or re-rendered. Running out of time on a patrol call is treated as a skip, not a guess — an unconsidered call would pollute the aggregated human signal.

## Properties that were actually demonstrated

- **Gapless resume.** The container was killed mid-stream: a **148-second** gap, **4,198 events** recovered via `Last-Event-ID` against Kafka-backed replay. Raw events are appended and fsynced *before* the resume pointer advances, so a crash cannot skip an unwritten event.
- **Settlement by reality.** Two handles made opposing calls on the same live edit. No revert landed inside the deadline, so "legit" was correct: ELO moved to **1024.31** and **992.31** from a 1000 start. Nothing about that outcome was simulated.
- **The revert parser at scale.** **264** revert incidents recorded from live traffic.
- **75 tests**, workspace-wide, no warnings — including a 27-comment revert corpus, 16 gate tests, and an SSE parser tested against frames split across chunk boundaries, CRLF, and back-to-back frames.

## Known gaps

**The receipts ledger is empty.** Zero bursts have ever cleared both gates in production. Gate 1 fires steadily — the watchlist of gate-1-only candidates is never short. Gate 2 is the binding constraint, and here is the measurement, taken live across every article the detector was tracking:

| Distinct editors in the window | Articles |
|---:|---:|
| 1 | 5,207 |
| 2 | 57 |
| 3 | 3 |

**5,267 article-windows; 98.9% have exactly one editor; the maximum anywhere is 3.** Gate 2 requires 5. The tally itself is healthy — those are 5,267 populated editor hashes with real per-editor counts, not an empty structure — so this is not an undercounting bug. Busy articles in ordinary traffic are overwhelmingly *one editor working rapidly*, which is precisely what Gate 2 exists to reject.

That is also the argument that the gate is calibrated correctly rather than merely strict: during a genuine breaking event a single article draws dozens of distinct editors within minutes, which is the entire signal Pulse is built to catch. On an ordinary afternoon, the correct output is silence.

So the thresholds were **not** lowered to manufacture a receipt. A ledger of bursts that were not bursts would be worth less than an empty one, and the distribution above is a more useful artifact than a forged row: it is a measurement of how rare the signal actually is.

**Call the Surge has never paid out**, for the same reason: the payout path is gated on a confirmation.

**First Responder has no UI.** The endpoint works; nothing on the web surfaces it.

## API

```
GET  /                      endpoint index
GET  /healthz
GET  /v1/live               SSE: `edit` and `confirmed` frames
GET  /v1/events             the receipts ledger
GET  /v1/events/:id
GET  /v1/controversy        radar: top articles by C_a, with war badges
GET  /v1/incidents?article= reverts behind a radar row
GET  /v1/watchlist          gate-1-only candidates (Surge leading indicator)
GET  /v1/patrol/queue       eligible edits to call
GET  /v1/leaderboard?mode=  patrol (ELO) | surge (points)
POST /v1/players            claim a handle, receive a signed cookie
GET  /v1/me
POST /v1/calls              a patrol call; 10-minute deadline
GET  /v1/calls/:id          settled? correct?
POST /v1/surge              stake points on an article
POST /v1/flag               First Responder
```

Identity is a handle plus an HMAC-signed cookie — no session table exists to be stolen. The signing key comes from `PULSE_SESSION_SECRET`; if it is unset the api signs with 32 bytes of `/dev/urandom` rather than a literal, because a default committed to a public repo is a publicly known signing key. **Set it in production**: without it, cookies do not survive a restart and, on a multi-container service, requests landing on a different container reject a valid cookie.

## Why this is hard

- **The stream never stops.** The ingest service holds one SSE connection open 24/7 and appends raw events before any processing. This structurally cannot be serverless, which is rather the point.
- **The signal is buried in noise.** Bots, template sweeps and maintenance runs dwarf human activity. The multi-gate design exists because naive `rate > threshold` cries wolf constantly — and, as *Known gaps* shows, a gate strict enough to be meaningful is also strict enough to stay quiet on an ordinary afternoon.
- **Ground truth is free but adversarial.** Settlement depends on parsing revert semantics out of freeform comments across conventions and languages, and every title, username and comment on the stream is attacker-controlled. The SPA therefore renders stream values via `textContent` only and never builds markup from them.

## Prior art

The core signal is peer-reviewed: *Wikipedia Live Monitor* (WWW '13, Steiner/van Hooland/Summers) showed concurrent cross-language edit spikes detect breaking news; later graph-based work improved on it. Then everyone published and walked away — the demos are dead Heroku URLs. Pulse rebuilds that research line as a living product and adds what none of the ancestors had: a permanent receipts ledger, a game settled by the stream's own ground truth, and a public conflict radar. The art-piece cousins (*Listen to Wikipedia*, *WikiStream*, the Recent Changes Map) visualize the stream but derive nothing from it.

## Stack

| Layer | Choice |
|---|---|
| Ingest / Detector / API | **Rust** — tokio, axum, sqlx, serde, regex. SSE is parsed by hand over `reqwest` (150 lines) rather than a client crate, so `Last-Event-ID` resume is ours to control. |
| Event bus & hot state | **Valkey** — streams as the bus (consumer groups, `XAUTOCLAIM` for crash recovery), sorted sets as sliding windows, hashes as editor tallies |
| Durable state | **PostgreSQL** |
| Frontend | Static SPA — Vite + TypeScript, 4 entries, SSE from the api |
| Platform | **Zerops** — 6 services (4 runtimes + Postgres + Valkey) and the private network between them |

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the design, [`PLAN.md`](./PLAN.md) for the build plan, and [`CLAUDE.md`](./CLAUDE.md) for the operational traps this repo has already paid for.

## Running locally

```bash
# Prereqs: Rust (stable), Docker, Node 20+
docker compose up -d          # postgres:16 + valkey:8
cp .env.example .env          # DATABASE_URL, VALKEY_URL, STREAM_URL

cargo run -p ingest           # hold the firehose open, append raw, publish to the bus
cargo run -p detector         # gates, classification, radar, settlement
cargo run -p api              # axum on :3000, SSE at /v1/live

cd web && npm ci && npm run dev
```

`cargo test --workspace` runs all 75 tests; none of them need Postgres or Valkey.

Detector tunables are read from the environment (`PULSE_K1`, `PULSE_EPSILON`, `PULSE_MIN_EDITORS`, `PULSE_MIN_REGISTERED`, `PULSE_WINDOW_SECS`, …) and deliberately **not** declared in `zerops.yaml`, because a key owned by that file cannot be overridden at service scope — which would turn every tuning change into a ~10-minute Rust rebuild.

## Deploying on Zerops

One `zerops.yaml` describes every service. `api` holds the monorepo and self-deploys with `deployFiles: [.]`; `ingest`, `detector` and `web` cross-deploy their build output from that same tree.

```
zerops_deploy targetService=api      # then detector, ingest, web
```

The ingest service must run continuously. A deploy of `api` **replaces** the container filesystem — commit before deploying; see [`CLAUDE.md`](./CLAUDE.md).

## AI-use disclosure

Built with AI assistance (Claude) for code generation, debugging, and documentation drafting — disclosed in full in the submission form. Architecture, detector design and algorithm choices are my own, and I can defend every line.

## Attribution & license

- Data: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams). Content is © its respective contributors, CC-BY-SA 4.0. Pulse displays metadata and links every item back to its source revision.
- Pulse itself: MIT.

## Roadmap

- [ ] Gate 3: cross-language confirmation via Wikidata QIDs — the precision upgrade, and the most likely route to a non-empty ledger
- [ ] A First Responder surface on the web, not just the API
- [ ] Propagation map: animated arcs showing attention travel between language communities
- [ ] Parquet archive on object storage + a public DataFusion SQL endpoint
- [ ] ORES/Lift Wing scores as a Vandal Patrol difficulty tier
- [ ] Webhook/RSS for confirmed events
- [ ] Replay harness: re-run every detector version against the full raw archive and publish precision/recall over time — the receipts philosophy applied to the detector itself
