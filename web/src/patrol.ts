/**
 * Vandal Patrol — PLAN.md Phase 3.
 *
 * See a live diff, call vandalism or legit in 10 seconds. The call is settled by
 * whether a real revert landed inside the deadline, so the stream grades you.
 *
 * Diffs are linked out to Wikipedia, never fetched or rendered here: PLAN.md is
 * explicit that the MVP does not proxy diffs.
 *
 * Everything on a candidate is attacker-controlled, so it lands via textContent.
 */
import { apiFetch, articleLabel, join, whoami, type Player } from "./api";

interface Candidate {
  article: string;
  wiki?: string;
  title?: string;
  title_url?: string;
  user?: string;
  anon?: boolean;
  comment?: string;
  delta?: number | null;
  rev_id?: number | null;
  diff_url?: string | null;
}

interface CallState {
  id: number;
  article: string;
  verdict: boolean;
  settled: boolean;
  correct: boolean | null;
  outcome: boolean | null;
  elo: number;
}

/** PLAN.md Phase 3: a 10-second decision. */
const DECIDE_SECS = 10;
/** Poll open calls at this cadence; deadlines are 10 minutes. */
const CALL_POLL_MS = 15_000;

const $ = (id: string) => document.getElementById(id);
const statusEl = $("status");
const statusText = $("status-text");

let me: Player | null = null;
let queue: Candidate[] = [];
let current: Candidate | null = null;
let ticking: number | null = null;
let remaining = DECIDE_SECS;
const openCalls = new Map<number, CallState>();

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

function show(id: string, visible: boolean): void {
  const el = $(id);
  if (el) el.hidden = !visible;
}

function renderWho(): void {
  const el = $("who");
  if (el && me) el.textContent = `${me.handle} · ELO ${Math.round(me.elo)} · ${me.points} pts`;
}

// ── the candidate card ──────────────────────────────────────────────────────

function renderCandidate(c: Candidate): void {
  const title = $("cand-title") as HTMLAnchorElement | null;
  if (title) {
    title.textContent = c.title ?? c.article;
    if (c.title_url) title.href = c.title_url;
  }
  const wiki = $("cand-wiki");
  if (wiki) wiki.textContent = c.wiki ?? "";

  const user = $("cand-user");
  if (user) {
    user.textContent = c.anon ? `${c.user ?? "?"} (anonymous)` : (c.user ?? "?");
    user.classList.toggle("anon", Boolean(c.anon));
  }

  const delta = $("cand-delta");
  if (delta) {
    const d = typeof c.delta === "number" ? c.delta : null;
    delta.textContent = d === null ? "·" : d > 0 ? `+${d} bytes` : `${d} bytes`;
    delta.className = "fact " + (d === null ? "" : d > 0 ? "pos" : "neg");
  }

  const comment = $("cand-comment");
  if (comment) comment.textContent = c.comment?.trim() || "(no edit summary)";

  const diff = $("cand-diff") as HTMLAnchorElement | null;
  if (diff) {
    if (c.diff_url) {
      diff.href = c.diff_url;
      diff.hidden = false;
    } else {
      diff.hidden = true;
    }
  }
}

function startTimer(): void {
  stopTimer();
  remaining = DECIDE_SECS;
  const el = $("timer");
  if (el) {
    el.textContent = String(remaining);
    el.classList.remove("urgent");
  }
  ticking = window.setInterval(() => {
    remaining -= 1;
    if (el) {
      el.textContent = String(Math.max(0, remaining));
      el.classList.toggle("urgent", remaining <= 3);
    }
    if (remaining <= 0) {
      // Out of time is a skip, not a guess — an unconsidered call would
      // pollute the aggregated human signal the controversy index consumes.
      stopTimer();
      void nextCandidate();
    }
  }, 1000);
}

function stopTimer(): void {
  if (ticking !== null) {
    window.clearInterval(ticking);
    ticking = null;
  }
}

async function refillQueue(): Promise<void> {
  const res = await apiFetch("/v1/patrol/queue?limit=40");
  if (!res.ok) {
    setStatus("error", `queue unavailable (HTTP ${res.status})`);
    return;
  }
  const data = (await res.json()) as { candidates: Candidate[] };
  // Only candidates we can actually settle against.
  queue = data.candidates.filter((c) => typeof c.rev_id === "number" && c.rev_id > 0);
}

async function nextCandidate(): Promise<void> {
  if (queue.length === 0) await refillQueue();
  current = queue.shift() ?? null;
  if (!current) {
    setStatus("idle", "no candidates right now — the filter is waiting for interesting edits");
    show("patrol", false);
    return;
  }
  show("patrol", true);
  setStatus("live", "live — call it");
  renderCandidate(current);
  setSettleMessage(null);
  startTimer();
}

function setSettleMessage(text: string | null, kind?: "win" | "loss" | "pending"): void {
  const box = $("settle");
  const el = $("settle-text");
  if (!box || !el) return;
  if (text === null) {
    box.hidden = true;
    return;
  }
  box.hidden = false;
  box.className = `settle ${kind ?? ""}`;
  el.textContent = text;
}

// ── calling ─────────────────────────────────────────────────────────────────

async function submit(verdict: boolean): Promise<void> {
  if (!current) return;
  stopTimer();
  const candidate = current;

  const res = await apiFetch("/v1/calls", {
    method: "POST",
    body: JSON.stringify({
      article: candidate.article,
      rev_id: candidate.rev_id,
      verdict,
    }),
  });
  if (!res.ok) {
    setSettleMessage(`could not record the call (HTTP ${res.status})`, "loss");
    return;
  }
  const call = (await res.json()) as { id: number; deadline: string };
  openCalls.set(call.id, {
    id: call.id,
    article: candidate.article,
    verdict,
    settled: false,
    correct: null,
    outcome: null,
    elo: me?.elo ?? 1000,
  });
  renderCalls();
  setSettleMessage(
    `call #${call.id} placed — settles when a revert lands, or at the 10-minute deadline`,
    "pending",
  );
  void nextCandidate();
}

function renderCalls(): void {
  const box = $("calls");
  if (!box) return;
  box.replaceChildren();
  if (openCalls.size === 0) {
    const p = document.createElement("p");
    p.className = "note";
    p.textContent = "No calls yet.";
    box.appendChild(p);
    return;
  }
  for (const c of [...openCalls.values()].reverse()) {
    const row = document.createElement("div");
    row.className = "call-row" + (c.settled ? (c.correct ? " won" : " lost") : "");

    const label = document.createElement("span");
    label.className = "call-article";
    label.textContent = articleLabel(c.article);
    row.appendChild(label);

    const verdict = document.createElement("span");
    verdict.className = "mono fact";
    verdict.textContent = c.verdict ? "called: vandalism" : "called: legit";
    row.appendChild(verdict);

    const outcome = document.createElement("span");
    outcome.className = "mono fact";
    outcome.textContent = c.settled
      ? `${c.outcome ? "was reverted" : "stood"} — ${c.correct ? "correct" : "wrong"}`
      : "awaiting reality…";
    row.appendChild(outcome);

    box.appendChild(row);
  }
}

/** Poll unsettled calls; reality arrives on its own schedule. */
async function pollCalls(): Promise<void> {
  for (const call of [...openCalls.values()]) {
    if (call.settled) continue;
    const res = await apiFetch(`/v1/calls/${call.id}`);
    if (!res.ok) continue;
    const data = (await res.json()) as CallState;
    if (!data.settled) continue;

    call.settled = true;
    call.correct = data.correct;
    call.outcome = data.outcome;
    openCalls.set(call.id, call);

    // Refresh our own rating: the settle moved it.
    me = (await whoami()) ?? me;
    renderWho();
    setSettleMessage(
      data.correct
        ? `call #${call.id}: correct — reality agreed. ELO now ${Math.round(data.elo)}`
        : `call #${call.id}: wrong — reality disagreed. ELO now ${Math.round(data.elo)}`,
      data.correct ? "win" : "loss",
    );
    renderCalls();
  }
}

// ── boot ────────────────────────────────────────────────────────────────────

async function boot(): Promise<void> {
  me = await whoami();
  if (!me) {
    show("join", true);
    setStatus("idle", "pick a handle to start");
    return;
  }
  show("join", false);
  renderWho();
  renderCalls();
  await nextCandidate();
  window.setInterval(() => void pollCalls(), CALL_POLL_MS);
}

$("join-form")?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const input = $("handle") as HTMLInputElement | null;
  const handle = input?.value.trim() ?? "";
  if (!handle) return;
  const player = await join(handle);
  const err = $("join-error");
  if (!player) {
    if (err) {
      err.hidden = false;
      err.textContent = "could not claim that handle — try another";
    }
    return;
  }
  me = player;
  if (err) err.hidden = true;
  show("join", false);
  renderWho();
  renderCalls();
  await nextCandidate();
  window.setInterval(() => void pollCalls(), CALL_POLL_MS);
});

$("btn-vandal")?.addEventListener("click", () => void submit(true));
$("btn-legit")?.addEventListener("click", () => void submit(false));
$("btn-skip")?.addEventListener("click", () => void nextCandidate());

void boot();
