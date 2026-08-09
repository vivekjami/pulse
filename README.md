# Pulse

**The world's largest collaborative document is a live sensor. Pulse listens to it.**

Wikipedia receives roughly ten edits per second, across ~300 languages, around the clock. When something happens in the real world — a public figure dies, a match ends, a government falls — the relevant articles erupt with edits, often minutes before news sites update. Pulse consumes the full Wikimedia edit firehose in real time, detects those eruptions with a multi-gate statistical detector, classifies them, timestamps them permanently, and turns the stream's own ground truth into a game anyone can play.

**Live:** `https://pulse-<project>.zerops.app` · **Built for:** The Zerops Challenge, Aug 8–9 2026 · **Data:** [Wikimedia EventStreams](https://stream.wikimedia.org/?doc) (CC-BY-SA, attribution below)

---

## What Pulse does

### 1. The Live Wall
Every edit on every Wikimedia wiki, streaming into your browser the moment it happens. Filter by language, watch byte deltas pulse, see anonymous edits light up. This is the ambient layer — the internet, breathing.

### 2. Burst detection with receipts
Pulse doesn't just show the stream — it *derives events from it*:

- **Gate 1 — Rate anomaly.** Per-article edit velocity vs. an exponentially-weighted baseline, normalized against the global stream rate (so bot floods don't trigger everything at once).
- **Gate 2 — Editor diversity.** 40 edits by one user is a rewrite or a fight. 40 edits by 25 distinct humans is *news*. Bursts must be driven by multiple distinct, non-bot editors.
- **Gate 3 — Cross-language confirmation** *(stretch)*. Real-world events erupt on multiple language wikis within minutes. Requiring the same Wikidata entity to burst on ≥3 wikis pushes precision from "decent" to "rarely wrong."

Confirmed bursts are classified from the stream's own annotations (category additions like `2026 deaths`, edit-comment keywords, infobox churn) into typed events: **death, disaster, sports, political, unclassified**.

Every detection is written to a permanent, public **receipts ledger**: *what* Pulse detected, *when*, with the evidence trail. Detected-at timestamps can't be faked retroactively — that ledger is the proof this works.

### 3. Conflict radar
Reverts self-identify in edit comments (`Undid revision N by X` names both parties). Pulse builds a live revert graph per article and surfaces:

- **Edit wars** — ping-pong revert cycles between 2–3 editors, flagged as they escalate.
- **Controversy index** — a decayed reverts-per-edit score; the "most fought-over pages right now" board is something even Wikipedia doesn't publicly surface.
- **Brigade flags** — many brand-new/anonymous accounts converging on one page within minutes.

### 4. The game — settled by reality
No invented mechanics. Every score settles against ground truth arriving on the same stream:

| Mode | You do | Ground truth |
|---|---|---|
| **Vandal Patrol** | See a live diff, call *vandalism* or *legit* in 10s | Was it reverted within N minutes? Auto-settled. ELO-rated. |
| **Call the Surge** | Stake points that an article confirm-bursts within the hour | Did Gate 1+2 fire? Paid only if you called it *before* confirmation. Brier-scored. |
| **First Responder** | Flag an article pre-confirmation | If the detector confirms within 30 min, your name goes on the event receipt itself. |

Vandal Patrol doubles as a labeling engine: aggregated human calls become a signal feeding the controversy index.

### 5. The archive *(stretch)*
Nightly compaction of the raw event log into date-partitioned Parquet on object storage, queryable via embedded Apache DataFusion:

```sql
SELECT wiki, count(*) FROM events
WHERE entity = 'Q762' AND ts > now() - interval '7 days'
GROUP BY wiki;
```

A public, SQL-queryable record of global attention — every burst, every edit war, every propagation trace. The stream is ephemeral; Pulse remembers.

---

## Why this is hard (and why it's interesting)

- **The stream never stops.** The ingest service holds an SSE connection open 24/7, uses `Last-Event-ID` to resume gaplessly across crashes and redeploys, and appends raw events before any processing. Kill the container; zero events lost. That's a demonstrable correctness property, not a claim.
- **The signal is buried in noise.** Bots, template sweeps, and maintenance runs dwarf human activity. The three-gate design exists because naive `rate > threshold` cries wolf constantly.
- **Ground truth is free but adversarial.** Settlement for Vandal Patrol depends on parsing revert semantics out of freeform edit comments across conventions and languages.

## Prior art (and what's new here)

The core signal is peer-reviewed: *Wikipedia Live Monitor* (WWW '13, Steiner/van Hooland/Summers) showed concurrent cross-language edit spikes detect breaking news; later graph-based work improved on it. Then everyone published and walked away — the demos are dead Heroku URLs. Pulse is that research line rebuilt as a living product, and adds what none of the ancestors had: a permanent receipts ledger, a game settled by the stream's own ground truth, a public conflict radar, and a queryable archive. The art-piece cousins (*Listen to Wikipedia*, *WikiStream*, the Recent Changes Map) visualize the stream but derive nothing from it.

## Stack

| Layer | Choice |
|---|---|
| Ingest / Detector / API | **Rust** — tokio, eventsource-client, sqlx, axum, serde |
| Event bus & hot state | **Valkey** (streams as bus, sorted sets as sliding windows) |
| Durable state | **PostgreSQL** |
| Archive *(stretch)* | Object storage (Parquet) + **Apache DataFusion** |
| Frontend | Static SPA (Vite + TS), SSE from the API |
| Platform | **Zerops** — every service, the private network between them, and the nightly cron |

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full design and [`PLAN.md`](./PLAN.md) for the build plan.

## Running locally

```bash
# Prereqs: Rust 1.79+, Docker (for Postgres + Valkey)
docker compose up -d          # postgres:16 + valkey:8
cp .env.example .env          # DATABASE_URL, VALKEY_URL
cargo run -p ingest           # connect to the firehose, start appending
cargo run -p detector         # gates + classification + settlement
cargo run -p api              # axum on :3000, SSE at /v1/live
cd web && npm i && npm run dev
```

No API keys required. The firehose is public.

## Deploying on Zerops

One `zerops.yaml` describes all services (see `ARCHITECTURE.md` §7). Push the repo, import the project, done. The ingest service **must** run continuously — this project structurally cannot exist on serverless, which is rather the point.

## AI-use disclosure

Built with AI assistance for code generation, debugging, and documentation drafting (disclosed in full in the submission form). Architecture, detector design, algorithm choices, and all final code are my own work and I can defend every line of it.

## Attribution & license

- Data: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams). Content edits are © their respective contributors, CC-BY-SA 4.0. Pulse displays diffs and metadata under those terms and links every item back to its source revision.
- Pulse itself: MIT.

## Roadmap

- [ ] Gate 3: cross-language confirmation via Wikidata QIDs
- [ ] Propagation map: animated arcs showing attention travel between language communities
- [ ] DataFusion SQL endpoint over the Parquet archive
- [ ] ORES/Lift Wing scores as an additional Vandal Patrol difficulty tier
- [ ] Webhook/RSS output for confirmed events ("Wikipedia believes X happened")