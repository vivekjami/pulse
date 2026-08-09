/**
 * Pulse web — the live wall.
 *
 * PLAN.md Phase 1: EventSource against the api's `/v1/live`. Rows carry a wiki
 * badge, the title, the byte delta (green/red), the user, with anonymous edits
 * highlighted. Auto-scroll, paused while the pointer is over the feed.
 *
 * Phase 2 adds `confirmed` frames: a burst that cleared both gates slides in as
 * an event card above the wall.
 *
 * Every value on a row is attacker-controlled — titles, usernames and comments
 * come off a public wiki — so text always lands via `textContent`. Nothing here
 * builds markup from stream data.
 */
import {
  apiBase,
  apiFetch,
  articleLabel,
  formatDetectedAt,
  join,
  KIND_ICON,
  splitArticle,
  whoami,
} from "./api";

/** Shape of the `edit` frame: the raw recentchange event the api forwards. */
interface RcEvent {
  meta?: { dt?: string };
  type?: string;
  title?: string;
  title_url?: string;
  user?: string;
  bot?: boolean;
  wiki?: string;
  length?: { old?: number | null; new?: number | null };
}

/** A burst that cleared both gates, as published on `pulse:bus:confirmed`. */
interface Confirmed {
  id: number;
  article: string;
  kind: string;
  detected_at: string;
  distinct_eds: number;
  peak_rate: number;
  wiki?: string;
  title?: string;
  title_url?: string;
  sample_comments?: string[];
}

/** Keep the DOM bounded — the stream never stops. */
const MAX_ROWS = 200;
/** Event cards are rare and worth keeping visible; cap the column anyway. */
const MAX_CARDS = 8;

const feed = document.getElementById("feed") as HTMLDivElement | null;
const statusEl = document.getElementById("status");
const statusText = document.getElementById("status-text");
const wallMeta = document.getElementById("wall-meta");
const cards = document.getElementById("cards") as HTMLDivElement | null;

/** Auto-scroll pauses while the pointer rests on the feed. */
let paused = false;
/** Events received in the current rate window. */
let windowCount = 0;
let totalCount = 0;

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

function byteDelta(ev: RcEvent): number | null {
  const len = ev.length;
  if (!len) return null;
  const before = typeof len.old === "number" ? len.old : 0;
  const after = typeof len.new === "number" ? len.new : 0;
  return after - before;
}

/** MediaWiki puts the IP in `user` for anonymous edits. */
function isAnon(user: string): boolean {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(user) || /^[0-9a-f:]+:[0-9a-f:]+$/i.test(user);
}

function el(tag: string, className: string, text?: string): HTMLElement {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function renderRow(ev: RcEvent): void {
  if (!feed) return;

  const row = el("div", "row");
  if (ev.bot) row.classList.add("is-bot");

  row.appendChild(el("span", "badge", ev.wiki ?? "?"));

  const title = ev.title ?? "(untitled)";
  if (ev.title_url) {
    const link = document.createElement("a");
    link.className = "title";
    link.textContent = title;
    link.href = ev.title_url;
    link.target = "_blank";
    // Untrusted outbound link: deny window.opener access and referrer leak.
    link.rel = "noopener noreferrer";
    row.appendChild(link);
  } else {
    row.appendChild(el("span", "title", title));
  }

  const delta = byteDelta(ev);
  const deltaNode = el("span", "delta");
  if (delta === null) {
    deltaNode.textContent = "·";
    deltaNode.classList.add("zero");
  } else {
    deltaNode.textContent = delta > 0 ? `+${delta}` : `${delta}`;
    deltaNode.classList.add(delta > 0 ? "pos" : delta < 0 ? "neg" : "zero");
  }
  row.appendChild(deltaNode);

  const user = ev.user ?? "";
  const userNode = el("span", "user", user);
  if (isAnon(user)) userNode.classList.add("anon");
  row.appendChild(userNode);

  feed.appendChild(row);
  while (feed.childElementCount > MAX_ROWS) {
    feed.removeChild(feed.firstElementChild!);
  }
  if (!paused) feed.scrollTop = feed.scrollHeight;
}

/**
 * An event card: type icon, article, detected-at, editor count, sample comments.
 * This is the moment the product earns its name, so it gets an entrance.
 */
function renderCard(ev: Confirmed): void {
  if (!cards) return;

  const card = el("article", `card kind-${ev.kind}`);

  const head = el("div", "card-head");
  head.appendChild(el("span", "card-icon", KIND_ICON[ev.kind] ?? "◦"));
  head.appendChild(el("span", "card-kind", ev.kind));
  head.appendChild(el("span", "card-when", formatDetectedAt(ev.detected_at)));
  card.appendChild(head);

  const title = ev.title ?? ev.article;
  if (ev.title_url) {
    const link = document.createElement("a");
    link.className = "card-title";
    link.textContent = title;
    link.href = ev.title_url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    card.appendChild(link);
  } else {
    card.appendChild(el("span", "card-title", title));
  }

  const facts = el("div", "card-facts");
  facts.appendChild(el("span", "fact", `${ev.distinct_eds} editors`));
  facts.appendChild(el("span", "fact", `${Math.round(ev.peak_rate)} edits/5m`));
  if (ev.wiki) facts.appendChild(el("span", "badge", ev.wiki));
  card.appendChild(facts);

  const samples = (ev.sample_comments ?? []).filter((c) => c.trim().length > 0).slice(0, 2);
  for (const comment of samples) {
    card.appendChild(el("p", "card-comment", comment));
  }

  const receipt = document.createElement("a");
  receipt.className = "card-receipt";
  receipt.textContent = `receipt #${ev.id} →`;
  receipt.href = "/events/";
  card.appendChild(receipt);

  cards.prepend(card);
  while (cards.childElementCount > MAX_CARDS) {
    cards.removeChild(cards.lastElementChild!);
  }
}

function connect(): void {
  const url = `${apiBase()}/v1/live`;
  setStatus("idle", "connecting to firehose…");

  const source = new EventSource(url);

  source.addEventListener("open", () => {
    setStatus("live", "live — Wikimedia EventStreams");
  });

  source.addEventListener("edit", (event) => {
    let ev: RcEvent;
    try {
      ev = JSON.parse((event as MessageEvent<string>).data) as RcEvent;
    } catch {
      return; // a frame we can't parse is not worth killing the wall over
    }
    windowCount += 1;
    totalCount += 1;
    renderRow(ev);
  });

  source.addEventListener("confirmed", (event) => {
    let ev: Confirmed;
    try {
      ev = JSON.parse((event as MessageEvent<string>).data) as Confirmed;
    } catch {
      return;
    }
    renderCard(ev);
  });

  source.addEventListener("error", () => {
    // EventSource reconnects on its own; report the gap honestly meanwhile.
    setStatus("error", "reconnecting…");
  });
}

function startRateMeter(): void {
  window.setInterval(() => {
    if (wallMeta) {
      const rate = (windowCount / 2).toFixed(1);
      wallMeta.textContent = `${rate} edits/s · ${totalCount.toLocaleString()} seen`;
    }
    windowCount = 0;
  }, 2000);
}

function wirePauseOnHover(): void {
  if (!feed) return;
  feed.addEventListener("mouseenter", () => {
    paused = true;
    feed.classList.add("paused");
  });
  feed.addEventListener("mouseleave", () => {
    paused = false;
    feed.classList.remove("paused");
  });
}

// ── Phase 4: the conflict radar ────────────────────────────────────────────

/** A row on the controversy board, as served by `GET /v1/controversy`. */
interface RadarRow {
  article: string;
  controversy: number;
  edit_war: boolean;
}

/** One revert, as served by `GET /v1/incidents?article=`. */
interface Incident {
  reverter: string;
  reverted: string;
  rev_id: number | null;
  at: string;
}

const RADAR_LIMIT = 12;
const RADAR_POLL_MS = 20_000;
const WATCH_LIMIT = 12;
const WATCH_POLL_MS = 15_000;
/** Matches the api's own STAKE_MIN..=STAKE_MAX guard; a bad value is a 400. */
const DEFAULT_STAKE = 10;

const radarList = document.getElementById("radar-list") as HTMLOListElement | null;
const radarMeta = document.getElementById("radar-meta");
const incidentsBox = document.getElementById("incidents") as HTMLDivElement | null;
const incidentsTitle = document.getElementById("incidents-title");
const incidentsBody = document.getElementById("incidents-body") as HTMLDivElement | null;
const watchStrip = document.getElementById("watch-strip") as HTMLDivElement | null;
const watchMeta = document.getElementById("watch-meta");

/** Link straight to the article's own history — the primary source for a claim. */
function historyUrl(article: string): string {
  const { wiki, title } = splitArticle(article);
  const host = wiki.endsWith("wiki") ? `${wiki.slice(0, -4)}.wikipedia.org` : "en.wikipedia.org";
  return `https://${host}/w/index.php?title=${encodeURIComponent(title.replace(/ /g, "_"))}&action=history`;
}

async function showIncidents(article: string): Promise<void> {
  if (!incidentsBox || !incidentsBody) return;
  incidentsBox.hidden = false;
  if (incidentsTitle) incidentsTitle.textContent = `reverts on ${articleLabel(article)}`;
  incidentsBody.replaceChildren(el("p", "note", "loading…"));

  let items: Incident[] = [];
  try {
    const res = await apiFetch(`/v1/incidents?article=${encodeURIComponent(article)}&limit=25`);
    if (!res.ok) throw new Error(String(res.status));
    items = ((await res.json()) as { incidents?: Incident[] }).incidents ?? [];
  } catch {
    incidentsBody.replaceChildren(el("p", "note", "could not load incidents"));
    return;
  }

  if (items.length === 0) {
    incidentsBody.replaceChildren(
      el("p", "note", "no parsed reverts on record — the index also counts raw edit pressure"),
    );
    return;
  }

  const rows = items.map((it) => {
    const row = el("div", "incident");
    row.appendChild(el("span", "incident-when", formatDetectedAt(it.at)));
    row.appendChild(el("span", "incident-who", it.reverter));
    row.appendChild(el("span", "incident-arrow", "reverted"));
    row.appendChild(el("span", "incident-who", it.reverted));
    return row;
  });
  incidentsBody.replaceChildren(...rows);
}

function renderRadar(rows: RadarRow[]): void {
  if (!radarList) return;
  if (rows.length === 0) {
    radarList.replaceChildren(el("li", "note", "no contested articles in the window yet"));
    return;
  }

  const items = rows.map((row) => {
    const li = el("li", "radar-row");
    if (row.edit_war) li.classList.add("at-war");

    li.appendChild(el("span", "radar-score", row.controversy.toFixed(1)));

    const link = document.createElement("a");
    link.className = "radar-title";
    link.textContent = articleLabel(row.article);
    link.href = historyUrl(row.article);
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    li.appendChild(link);

    if (row.edit_war) li.appendChild(el("span", "war-badge", "edit war"));

    const more = document.createElement("button");
    more.type = "button";
    more.className = "ghost";
    more.textContent = "incidents";
    more.addEventListener("click", () => void showIncidents(row.article));
    li.appendChild(more);

    return li;
  });
  radarList.replaceChildren(...items);
}

async function pollRadar(): Promise<void> {
  try {
    const res = await apiFetch(`/v1/controversy?limit=${RADAR_LIMIT}`);
    if (!res.ok) throw new Error(String(res.status));
    const body = (await res.json()) as { articles?: RadarRow[] };
    const rows = body.articles ?? [];
    renderRadar(rows);
    const wars = rows.filter((r) => r.edit_war).length;
    if (radarMeta) {
      radarMeta.textContent = wars > 0 ? `${rows.length} tracked · ${wars} at war` : `${rows.length} tracked`;
    }
  } catch {
    if (radarMeta) radarMeta.textContent = "radar offline";
  }
}

// ── Phase 5: the watchlist strip + stake button ────────────────────────────

/** A gate-1-only candidate, as served by `GET /v1/watchlist`. */
interface Candidate {
  article: string;
  seen_at_ms: number;
}

/** Cached so the strip can show "staked" without a round trip per render. */
const staked = new Set<string>();

/**
 * Place a Surge bet, claiming a handle first if the visitor has no cookie yet.
 * The prompt is deliberate: a bet must be attributable to settle against a
 * confirmation, and the api refuses an unauthenticated stake with a 401.
 */
async function stake(article: string, button: HTMLButtonElement): Promise<void> {
  button.disabled = true;
  try {
    let player = await whoami();
    if (!player) {
      const handle = window.prompt("Pick a handle to bet under:")?.trim();
      if (!handle) return;
      player = await join(handle);
      if (!player) {
        button.textContent = "handle taken";
        return;
      }
    }

    const res = await apiFetch("/v1/surge", {
      method: "POST",
      body: JSON.stringify({ article, stake: DEFAULT_STAKE }),
    });
    if (res.status === 402) {
      button.textContent = "no points";
      return;
    }
    if (!res.ok) {
      button.textContent = "failed";
      return;
    }
    staked.add(article);
    button.textContent = `staked ${DEFAULT_STAKE}`;
    button.classList.add("staked");
  } catch {
    button.textContent = "failed";
  } finally {
    // A settled or rejected bet stays visible; only a live bet locks the button.
    if (!staked.has(article)) button.disabled = false;
  }
}

function renderWatchlist(items: Candidate[]): void {
  if (!watchStrip) return;
  if (items.length === 0) {
    watchStrip.replaceChildren(
      el("p", "note", "no candidates on the watchlist — gate 1 is quiet right now"),
    );
    return;
  }

  const chips = items.map((item) => {
    const chip = el("div", "chip");
    chip.setAttribute("role", "listitem");

    const link = document.createElement("a");
    link.className = "chip-title";
    link.textContent = articleLabel(item.article);
    link.href = historyUrl(item.article);
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    chip.appendChild(link);

    const seen = new Date(item.seen_at_ms);
    if (!Number.isNaN(seen.getTime())) {
      chip.appendChild(el("span", "chip-when", formatDetectedAt(seen.toISOString())));
    }

    const button = document.createElement("button");
    button.type = "button";
    button.className = "stake";
    if (staked.has(item.article)) {
      button.textContent = `staked ${DEFAULT_STAKE}`;
      button.classList.add("staked");
      button.disabled = true;
    } else {
      button.textContent = `stake ${DEFAULT_STAKE}`;
      button.addEventListener("click", () => void stake(item.article, button));
    }
    chip.appendChild(button);

    return chip;
  });
  watchStrip.replaceChildren(...chips);
}

async function pollWatchlist(): Promise<void> {
  try {
    const res = await apiFetch(`/v1/watchlist?limit=${WATCH_LIMIT}`);
    if (!res.ok) throw new Error(String(res.status));
    const body = (await res.json()) as { candidates?: Candidate[] };
    const items = body.candidates ?? [];
    renderWatchlist(items);
    if (watchMeta) watchMeta.textContent = `${items.length} candidate${items.length === 1 ? "" : "s"}`;
  } catch {
    if (watchMeta) watchMeta.textContent = "watchlist offline";
  }
}

document.getElementById("incidents-close")?.addEventListener("click", () => {
  if (incidentsBox) incidentsBox.hidden = true;
});

wirePauseOnHover();
startRateMeter();
connect();

void pollRadar();
void pollWatchlist();
window.setInterval(() => void pollRadar(), RADAR_POLL_MS);
window.setInterval(() => void pollWatchlist(), WATCH_POLL_MS);
