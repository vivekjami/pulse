# Pulse — Execution Plan

Budgeted for a ~14-hour solo build window ending at the Aug 9 submission deadline. Each phase has an **exit criterion** — a demoable fact, not a feeling. If a phase blows its budget by >50%, take its cut line and move on. The plan is ordered so that *every phase boundary is a valid submission*: if you stop at the end of any phase from 3 onward, you have a complete, honest project.

**Standing rules**

1. Deploy to Zerops at the END of every phase, not once at the end. The deploy loop is where hackathons die; keep it warm.
2. Commit at every exit criterion. Screenshot/screen-record anything that moves — you're collecting build-post footage all day, not at 11 PM.
3. Raw before smart: every event is appended before anything computes on it.
4. The register step for the challenge, if not done: do it NOW, before Phase 0.

---

## Phase 0 — Skeleton & first deploy (0:00–1:00)

- Cargo workspace: `crates/{ingest,detector,api,common}`; `common` holds the `RcEvent` serde model + config.
- `web/`: Vite + TS scaffold, one dark page, "PULSE" and an empty feed div.
- `docker-compose.yml`: postgres:16 + valkey:8 for local.
- Zerops project via ZCP: create services (api, web, db, cache), let the agent write `zerops.yaml`, push, confirm the placeholder API answers `/healthz` on a live URL.

**Exit:** live URL returns `{"ok":true}` from Rust on Zerops. *Cut line: none — this phase cannot be skipped, but ZCP should make it fast.*

## Phase 1 — The firehose, tamed (1:00–3:00)

- `ingest`: `eventsource-client` (or reqwest + manual SSE parse) → `RcEvent` → filter `type in {edit,new,categorize}` → gzip JSONL hourly append → `XADD pulse:bus:edits`.
- Persist `last_event_id` to Valkey every 200 events; on boot, send `Last-Event-ID` and log the replay count.
- `api`: subscribe Valkey → `broadcast` channel → `GET /v1/live` SSE, sampled to ≤20 frames/s.
- `web`: EventSource → the live wall. Rows: wiki badge, title, byte delta (green/red), user, anon highlight. Auto-scroll, pause on hover.

**Exit (the morale moment):** open the live URL and watch the world edit Wikipedia in real time, on your infrastructure. **Kill the ingest container from the Zerops console, restart it, show the gap replay in logs — record this clip now; it's the centerpiece of the build post.**
*Cut line: skip hourly rotation — one growing JSONL file is fine for the weekend.*

## Phase 2 — The detector: gates 1+2 + receipts (3:00–6:30) — THE CORE

- `detector`: `XREADGROUP` consumer. Maintain Valkey windows (`ZADD`/`ZREMRANGEBYSCORE`), distinct-editor sets, global rate counters, per-article EWMA.
- Gate 1 (rate vs. EWMA, global-normalized), Gate 2 (diversity: ≥5 non-bot, ≥2 registered, top-share ≤0.5). Constants in config, tunable without rebuild.
- Classification v1: category-add patterns + comment keywords → `death|sports|political|disaster|unclassified`.
- On confirm: INSERT into `events` with full evidence JSONB; `XADD pulse:bus:confirmed`; api pushes a `confirmed` frame; web renders an **event card** (type icon, article, detected-at, editor count, sample comments) sliding in above the wall.
- **Receipts page**: `/events` listing every detection with its permanent timestamp. Plain table, newest first. Do not skip this — it's the credibility organ.
- Tune live: watch `unconfirmed` candidates in logs, adjust `k1` until obvious junk stops passing. Budget 45 min for tuning alone.

**Exit:** at least one confirmed burst on the receipts page with believable evidence. (At ~10 ev/s globally, several real bursts per hour is normal — sports and deaths never stop.)
*Cut lines, in order: drop classification (everything `unclassified`); drop the top-share check; never drop the receipts page.*

## Phase 3 — Vandal Patrol (6:30–9:00)

- Eligibility filter: non-bot main-namespace edits, |delta| in a suspicious band or anon + negative delta — enough to keep the queue interesting.
- `POST /v1/players` (handle + signed cookie), `POST /v1/calls` with 10-min deadline → `ZADD pulse:settle`.
- Settlement in detector: strong revert-regex match on the same article naming the rev/user within deadline → outcome; ELO update (K=32, expected score vs. a fixed 1200 "house"); leaderboard endpoint + page.
- Web: patrol mode — card with title, comment, delta, diff link (link out to Wikipedia's diff URL — do NOT fetch/render diffs server-side in MVP), two buttons, 10-second timer, satisfying settle animation when truth arrives.

**Exit:** you (as two different handles) play a round on the live URL; a call auto-settles when a real revert lands; leaderboard moves.
*Cut lines: fixed stake instead of ELO; skip the timer animation. Do not cut auto-settlement — the "reality grades you" loop IS the feature.*

## Phase 4 — Conflict radar (9:00–10:30)

- Revert regex module (with a unit-test corpus of ~30 real comments captured from your own raw log — you have hours of it by now).
- `revert_incidents` writes; controversy index in Valkey (decayed counters); A↔B cycle scan over last-20 edges.
- Web: "Fought over right now" panel — top articles by C_a, war badge on active cycles, click-through to incident list.

**Exit:** the panel shows real contested articles (there is *always* something).
*Cut line: skip cycle detection, ship the controversy leaderboard alone.*

## Phase 5 — Call the Surge (10:30–11:30)

- `POST /v1/surge`; on every confirmation, detector settles bets where `placed_at < detected_at < expires_at`; expiry sweep zeroes the rest. Points ledger on the leaderboard.
- Web: a "watchlist" strip of gate-1-only candidates (public leading indicators — this is what makes the bet skill, not luck) + stake button.

**Exit:** a bet placed on a candidate settles when/if it confirms.
*Cut line: cut the whole phase. Patrol alone carries the game story; Surge is described in the README as designed-and-scaffolded.*

## Phase 6 — Polish, story, submission (11:30–14:00) — PROTECTED, DO NOT INVADE

**6a. Visual pass (45 min).** Dark theme, one accent color, monospace numerals, event-type icons, favicon, OG tags. The wall should *feel* alive: subtle pulse on new frames. Nothing more.

**6b. The build post (45 min).** Required by the rules — it's a judged track AND a submission requirement. Structure:
  1. Hook: the receipts screenshot — "Pulse detected X at 14:32, N minutes before <outlet> tweeted it" (you'll have at least one by now).
  2. The kill-the-container clip from Phase 1 (gapless resume).
  3. 30–60 s screen recording: wall → event card lands → patrol round settles.
  4. One diagram (crop from ARCHITECTURE.md) + two sentences on why this can't be serverless — ingest must live 24/7, and the private-network pipeline (ingest→Valkey→detector→Postgres→api) is the Zerops story.
  5. Live URL + repo. Tag **@WeMakeDevs** and **@zeropsio**. Project name, what it does, video, deployment link, how Zerops is used — every required element from the checklist.

**6c. Submission form (30 min).** Repo public, README links checked, live URL up, post URL, **every AI tool disclosed**, demo video uploaded. Re-read the AI policy section once before submitting.

**6d. Buffer (60 min).** You will need it. If somehow not: capture the DataFusion `/v1/sql` stretch or the Wikidata gate-3 lookup — but only from inside an intact buffer.

---

## Submission checklist (from the challenge rules)

- [ ] Registered before building
- [ ] Live URL reachable, stays up through judging (check Zerops service auto-restart is on)
- [ ] Zerops meaningfully involved (it is: 6 services, private network, cron)
- [ ] Public repo
- [ ] Public build post: name, what it does, video, live link, how Zerops is used, @WeMakeDevs + @zeropsio tagged
- [ ] Submission form: repo, live URL, demo, post link, AI tools disclosed
- [ ] You can explain every architectural decision to a judge (you designed them; you can)

## Judge Q&A prep (10 minutes, while something compiles)

- *"Hasn't this been done?"* — In 2013, in a WWW paper, on a Heroku URL that's been dead a decade. Pulse productizes that research line and adds receipts, settlement-by-ground-truth, the conflict radar, and persistence. You know the prior art better than the asker.
- *"Why Rust?"* — A 24/7 stream consumer with sliding windows wants predictable memory and no GC pauses; also, it's the language the rest of my systems work is in.
- *"Why can't this be a Lambda?"* — The SSE connection IS the product. Stateless request/response cannot hold it.
- *"What breaks first at 100× load?"* — Valkey window memory; the design already gates window creation on ≥2 edits/15 min, and partitioning detector consumers by wiki is the scale-out path.

## Post-hackathon (so the README roadmap is honest)

Gate 3 via Wikidata QIDs → propagation arcs → DataFusion public SQL → ORES difficulty tiers → RSS/webhook for confirmed events → replay harness: re-run every detector version against the full raw archive and publish precision/recall over time. That last one turns Pulse into a self-measuring system — the same receipts philosophy applied to itself.