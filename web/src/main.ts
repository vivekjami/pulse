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
import { apiBase, formatDetectedAt, KIND_ICON } from "./api";

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

wirePauseOnHover();
startRateMeter();
connect();
