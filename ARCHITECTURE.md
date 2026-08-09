# Pulse — Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                     ZEROPS PROJECT                  │
                    │                  (private network)                  │
                    │                                                     │
 Wikimedia          │  ┌─────────┐   XADD    ┌──────────┐                 │
 EventStreams ──SSE──▶ │ ingest  │─────────▶│  Valkey   │                 │
 (recentchange)     │  │  (Rust) │           │ streams + │                 │
                    │  └────┬────┘           │ counters  │                 │
                    │       │ raw append     └────┬─────┘                 │
                    │       ▼                     │ XREADGROUP            │
                    │  ┌─────────┐                ▼                       │
                    │  │ object  │           ┌──────────┐    ┌─────────┐  │
                    │  │ storage │◀──cron────│ detector │───▶│Postgres │  │
                    │  │(Parquet)│  compact  │  (Rust)  │    │         │  │
                    │  └────┬────┘           └────┬─────┘    └────┬────┘  │
                    │       │ DataFusion          │ publish       │       │
                    │       ▼                     ▼               │       │
                    │  ┌──────────────────────────────────┐       │       │
                    │  │            api (Rust/axum)       │◀──────┘       │
                    │  │   REST + SSE fanout + SQL(read)  │               │
                    │  └───────────────┬──────────────────┘               │
                    │                  │                                  │
                    │  ┌───────────────▼──────────────────┐               │
                    │  │        web (static SPA)          │               │
                    │  └──────────────────────────────────┘               │
                    └─────────────────────────────────────────────────────┘
```

## 1. Services

| Service | Type | Job | Why it's separate |
|---|---|---|---|
| `ingest` | Rust, long-running | Hold the SSE connection, parse, filter, append raw, XADD to bus | Must never block on downstream compute; simplest possible hot path |
| `detector` | Rust, long-running | Consume bus, run gates, classify, settle games, write Postgres | CPU-bound windows; can crash/redeploy freely — bus buffers |
| `api` | Rust/axum | REST + SSE fanout to browsers; read-only SQL over archive | Public surface; scales independently |
| `db` | PostgreSQL 16 | Durable events, receipts, players, calls | — |
| `cache` | Valkey 8 | Event bus (streams), sliding windows, pub/sub | — |
| `storage` *(stretch)* | Object storage | Raw log + nightly Parquet partitions | — |
| `web` | Static | Vite-built SPA | — |

**MVP collapse:** if time gets tight, `ingest` and `detector` merge into one binary (`engine`) with two tokio task groups. The bus stays — it's an in-process channel + Valkey mirror, so the split back into two services is a config change, not a rewrite. Minimum viable topology: `engine` + `api` + `db` + `cache` + `web`.

## 2. The source stream

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange` — SSE, no auth, no key.
- ~10 events/sec global. Each event (JSON) carries: `title`, `wiki`, `user`, `bot` (flag), `type` (`edit|new|categorize|log`), `comment`, `revision.old/new` ids, `length.old/new` (byte delta), `timestamp`, `server_url`, `meta.id`/`meta.dt`.
- **Resumability:** SSE `Last-Event-ID` header. The service is Kafka-backed; on reconnect, send the last consumed event's ID (or a timestamp) and the server replays from that position. Persist the last-acked ID in Valkey (`pulse:ingest:last_event_id`) every N events; on boot, resume from it. This is the gapless-ingest guarantee — demo it by killing the container.
- **Backpressure rule:** ingest does *only* parse → raw-append → XADD. All computation lives downstream.

## 3. Data model

### 3.1 Valkey (hot state + bus)

```
# Bus
XADD  pulse:bus:edits * payload <json>           # detector: XREADGROUP grp-detector
XADD  pulse:bus:confirmed * payload <json>       # api: fanout to SSE clients

# Sliding windows (per hot article; article key = "{wiki}:{title}")
ZADD  pulse:win:{article}  <ts_ms> <event_uuid>  # ZREMRANGEBYSCORE to trim to 15 min
SADD  pulse:eds:{article}:{bucket10m} <user>     # distinct-editor sets, EXPIRE 30m
INCR  pulse:global:rate:{bucket1m}               # global stream rate, EXPIRE 10m

# Baselines
HSET  pulse:ewma <article> <rate>                # per-article EWMA (only articles seen in 24h)

# Vandal Patrol settlement queue
ZADD  pulse:settle  <deadline_ts> <call_id>      # detector polls due settlements

# Resume + misc
SET   pulse:ingest:last_event_id <id>
```

Only articles with ≥2 edits in 15 min get window keys — everything else stays out of memory. Expected working set: low thousands of keys.

### 3.2 PostgreSQL

```sql
-- Confirmed detections: the receipts ledger
CREATE TABLE events (
  id            BIGSERIAL PRIMARY KEY,
  article       TEXT NOT NULL,             -- "{wiki}:{title}"
  wikidata_qid  TEXT,                      -- gate-3 stretch
  kind          TEXT NOT NULL,             -- death|disaster|sports|political|unclassified
  detected_at   TIMESTAMPTZ NOT NULL,      -- THE timestamp. Never updated.
  gate1_at      TIMESTAMPTZ,
  gate2_at      TIMESTAMPTZ,
  peak_rate     REAL,
  distinct_eds  INT,
  evidence      JSONB NOT NULL,            -- sample comments, categories, rev ids
  first_flagger BIGINT REFERENCES players(id)   -- First Responder credit
);
CREATE INDEX ON events (detected_at DESC);

-- Conflict radar
CREATE TABLE revert_incidents (
  id          BIGSERIAL PRIMARY KEY,
  article     TEXT NOT NULL,
  reverter    TEXT NOT NULL,
  reverted    TEXT NOT NULL,
  rev_id      BIGINT,
  at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON revert_incidents (article, at DESC);

CREATE TABLE players (
  id          BIGSERIAL PRIMARY KEY,
  handle      TEXT UNIQUE NOT NULL,        -- no auth for MVP: handle + signed cookie
  elo         REAL NOT NULL DEFAULT 1000,
  points      BIGINT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- Vandal Patrol
CREATE TABLE calls (
  id          BIGSERIAL PRIMARY KEY,
  player_id   BIGINT REFERENCES players(id),
  article     TEXT NOT NULL,
  rev_id      BIGINT NOT NULL,
  verdict     BOOLEAN NOT NULL,            -- true = "vandalism"
  called_at   TIMESTAMPTZ NOT NULL,
  deadline    TIMESTAMPTZ NOT NULL,        -- called_at + 10 min
  outcome     BOOLEAN,                     -- reverted within window?
  settled_at  TIMESTAMPTZ
);

-- Call the Surge
CREATE TABLE surge_bets (
  id          BIGSERIAL PRIMARY KEY,
  player_id   BIGINT REFERENCES players(id),
  article     TEXT NOT NULL,
  stake       INT NOT NULL,
  placed_at   TIMESTAMPTZ NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,        -- placed_at + 60 min
  won         BOOLEAN,                     -- confirmed AFTER placed_at, before expiry
  settled_at  TIMESTAMPTZ
);
```

Positions/edits do **not** go to Postgres — only derived events and game state. Raw edits go to the append-only log (§6).

## 4. The detector

### Gate 1 — rate anomaly
Per article `a`, maintain EWMA rate `μ_a` (α = 0.3, 1-min buckets). Let `r_a` = current 5-min window rate, `G` = global stream rate vs. its own 1-h baseline `μ_G`.

```
fire iff  (r_a / max(μ_a, ε))  >  k1 · (G / μ_G)   AND   r_a ≥ 6 edits / 5 min
```

The `G/μ_G` term is the bot-flood normalizer: when the whole stream doubles, per-article thresholds double with it. Start `k1 = 8`; tune live against known-hot articles.

### Gate 2 — editor diversity
Within the same window:
```
fire iff  distinct_non_bot_editors ≥ 5
      AND registered_editors ≥ 2
      AND top_editor_share ≤ 0.5
```
Drop `bot == true` events before they ever reach windows. Gate 2 kills single-author rewrites and two-person fights (which route to the conflict radar instead — a rejected burst with 2 dominant editors and reverts is an *edit-war candidate*, so the gates feed both products).

### Gate 3 — cross-language (stretch)
Resolve `title → QID` (one cheap Wikidata API call per gate-2 survivor, cached in Valkey). Confirm iff the same QID passes gates 1–2 on ≥3 wikis inside 30 min. Ship behind a flag; without it, label events "unconfirmed" vs "confirmed" honestly.

### Classification
On confirmation, inspect the window's evidence:
- `categorize` events adding `... deaths`, `... disasters`, election/sport category patterns → typed.
- Comment keyword lists per type (`died`, `death date`, `final score`, `resigned`, ...), en + hi to start.
- Else `unclassified`. Precision over coverage — a wrong "death" label is worse than no label.

### Revert parsing (conflict radar + settlement)
Regexes over comments, ordered by confidence:
```
Undid revision (\d+) by \[\[Special:Contributions/([^|]+)\|      → reverter, reverted, rev_id
^Reverted \d+ edits? by \[\[Special:Contribs/([^|]+)             → rollback
(?i)\brvv?\b|revert|vandal                                       → weak signal (radar only)
```
Strong matches settle Vandal Patrol calls (`outcome = matched rev_id or article+user within deadline`) and append to `revert_incidents`. Weak matches only bump the controversy index: `C_a = decayed(reverts) / decayed(edits)` over 1 h, λ = 10 min half-life.

### Edit-war cycle detection
Per article keep the last 20 revert edges; an A↔B (or A→B→C→A) cycle with ≥3 edges in 30 min = incident. This is a 20-element scan, not a graph library.

## 5. API surface

```
GET  /v1/live                 SSE: multiplexed {edit|confirmed|war|leaderboard} frames
GET  /v1/events?limit=&kind=  receipts ledger, newest first
GET  /v1/events/:id           full evidence bundle
GET  /v1/controversy          current top-20 by C_a
POST /v1/players              {handle} → signed cookie
POST /v1/calls                {rev_id, article, verdict}
POST /v1/surge                {article, stake}
POST /v1/flag                 First Responder flag
GET  /v1/leaderboard?mode=
GET  /v1/sql?q=               (stretch) read-only DataFusion over archive; SELECT-only,
                              validated via sqlparser AST, 5 s timeout, row cap
```

SSE fanout: api holds a `tokio::sync::broadcast` fed by a Valkey subscriber; browsers get one connection, frames tagged by type. Cap the live-wall frame rate to ~20/s (sample, don't queue).

## 6. Raw log & archive (stretch)

- Ingest appends every raw event as JSONL, gzipped and rotated hourly, pushed to object storage: `raw/dt=2026-08-09/hh=14/part-*.jsonl.gz`. This is the "never destroy raw data" rule — every detector improvement can be replayed against history.
- Nightly Zerops cron runs the `compactor` (a detector subcommand): JSONL → columnar Parquet, partitioned by date, schema `{ts, wiki, title, user, bot, kind, delta, comment_hash, rev_id}`.
- `api` embeds DataFusion registering `events/` and `raw/` partitions as external tables for `/v1/sql`.

## 7. zerops.yaml (shape)

```yaml
zerops:
  - setup: ingest
    build: { base: rust@1, buildCommands: ["cargo build --release -p ingest"], deployFiles: target/release/ingest }
    run:   { base: ubuntu@24, start: ./ingest }          # no port; pure worker
  - setup: detector
    build: { base: rust@1, buildCommands: ["cargo build --release -p detector"], deployFiles: target/release/detector }
    run:   { base: ubuntu@24, start: ./detector }
  - setup: api
    build: { base: rust@1, buildCommands: ["cargo build --release -p api"], deployFiles: target/release/api }
    run:
      base: ubuntu@24
      ports: [{ port: 3000, httpSupport: true }]
      start: ./api
  - setup: web
    build: { base: nodejs@22, buildCommands: ["npm ci", "npm run build"], deployFiles: dist/~ }
    run:   { base: static }
# db (postgres@16), cache (valkey@8), storage (object storage) created as managed services;
# connection strings arrive as env vars over the private network.
```

Cron (compactor) attaches to the detector service's scheduled-jobs config. Exact syntax: let ZCP write it, then read what it wrote.

## 8. Failure modes, considered

| Failure | Behavior |
|---|---|
| Ingest crash / redeploy | Resume from `last_event_id`; Kafka-backed replay closes the gap. **Demo this.** |
| Detector crash | Bus (Valkey stream + consumer group) buffers; XAUTOCLAIM pending entries on restart |
| Valkey restart | Windows rebuild within 15 min organically; bus loss = brief detection blindness, raw log intact |
| Postgres down | Detector buffers confirmed events on the bus; api serves stale reads |
| Stream quiet day | Vandalism + reverts never stop; Patrol and radar carry the demo; receipts show history |
| Comment-format drift | Regexes versioned in one module with a test corpus of real comments captured on day one |

## 9. What is deliberately NOT here

No Kafka (Valkey streams suffice at 10 ev/s), no ML (the stream annotates itself), no auth beyond handle+cookie (hackathon), no per-edit Postgres writes (raw log owns history), no k8s manifests (Zerops owns topology).