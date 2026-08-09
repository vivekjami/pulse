/**
 * Pulse web — the live wall.
 *
 * PLAN.md Phase 1: EventSource against the api's `/v1/live`. Rows carry a wiki
 * badge, the title, the byte delta (green/red), the user, with anonymous edits
 * highlighted. Auto-scroll, paused while the pointer is over the feed.
 *
 * Every value on a row is attacker-controlled — titles, usernames and comments
 * come off a public wiki — so text always lands via `textContent`. Nothing here
 * builds markup from stream data.
 */

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

/** Keep the DOM bounded — the stream never stops. */
const MAX_ROWS = 200;

const feed = document.getElementById("feed") as HTMLDivElement | null;
const statusEl = document.getElementById("status");
const statusText = document.getElementById("status-text");
const wallMeta = document.getElementById("wall-meta");

/** Auto-scroll pauses while the pointer rests on the feed. */
let paused = false;
/** Events received in the current rate window. */
let windowCount = 0;
let totalCount = 0;

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

/**
 * Where the api lives. Zerops bakes `VITE_API_BASE` at build time from the
 * api service's own subdomain; if that reference didn't resolve we derive it
 * from our own hostname, which also makes `npm run dev` work locally.
 */
function apiBase(): string {
  const configured = import.meta.env.VITE_API_BASE as string | undefined;
  if (configured && configured.startsWith("http")) {
    return configured.replace(/\/+$/, "");
  }
  const host = window.location.hostname;
  const [sub, ...rest] = host.split(".");
  if (sub.startsWith("web-") && rest.length > 0) {
    return `${window.location.protocol}//${sub.replace(/^web-/, "api-")}-3000.${rest.join(".")}`;
  }
  return "http://localhost:3000";
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
