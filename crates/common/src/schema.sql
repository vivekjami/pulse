-- ARCHITECTURE.md §3.2. Idempotent so detector and api can each apply it on
-- boot without a start-order dependency.
--
-- Positions/edits do NOT go to Postgres — only derived events and game state.
-- Raw edits live in the append-only log (§6).

-- players is created first: events.first_flagger references it.
CREATE TABLE IF NOT EXISTS players (
  id          BIGSERIAL PRIMARY KEY,
  handle      TEXT UNIQUE NOT NULL,        -- no auth for MVP: handle + signed cookie
  elo         REAL NOT NULL DEFAULT 1000,
  points      BIGINT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- Confirmed detections: the receipts ledger.
CREATE TABLE IF NOT EXISTS events (
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
CREATE INDEX IF NOT EXISTS events_detected_at_idx ON events (detected_at DESC);

-- Conflict radar (Phase 4).
CREATE TABLE IF NOT EXISTS revert_incidents (
  id          BIGSERIAL PRIMARY KEY,
  article     TEXT NOT NULL,
  reverter    TEXT NOT NULL,
  reverted    TEXT NOT NULL,
  rev_id      BIGINT,
  at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS revert_incidents_article_at_idx ON revert_incidents (article, at DESC);

-- Vandal Patrol (Phase 3).
CREATE TABLE IF NOT EXISTS calls (
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

-- Call the Surge (Phase 5).
CREATE TABLE IF NOT EXISTS surge_bets (
  id          BIGSERIAL PRIMARY KEY,
  player_id   BIGINT REFERENCES players(id),
  article     TEXT NOT NULL,
  stake       INT NOT NULL,
  placed_at   TIMESTAMPTZ NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,        -- placed_at + 60 min
  won         BOOLEAN,                     -- confirmed AFTER placed_at, before expiry
  settled_at  TIMESTAMPTZ
);
