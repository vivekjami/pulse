/**
 * The receipts page — PLAN.md Phase 2's "credibility organ".
 *
 * A plain table, newest first, showing every detection and its permanent
 * timestamp. All values arrive from a public wiki, so everything is inserted
 * with `textContent`; nothing here builds markup from stream data.
 */
import { apiBase, formatDetectedAt, KIND_ICON, splitArticle } from "./api";

interface ReceiptEvidence {
  categories?: string[];
  sample_comments?: string[];
  gate1?: { window_edits?: number; anomaly?: number; threshold?: number };
  gate2?: { distinct_editors?: number; registered_editors?: number; top_editor_share?: number };
  title_url?: string;
}

interface Receipt {
  id: number;
  article: string;
  kind: string;
  detected_at: string;
  peak_rate: number | null;
  distinct_eds: number | null;
  evidence: ReceiptEvidence;
}

const statusEl = document.getElementById("status");
const statusText = document.getElementById("status-text");
const meta = document.getElementById("ledger-meta");
const body = document.getElementById("ledger-body") as HTMLTableSectionElement | null;

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

function cell(row: HTMLTableRowElement, text: string, className?: string): HTMLTableCellElement {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  row.appendChild(td);
  return td;
}

function renderRow(r: Receipt): HTMLTableRowElement {
  const { wiki, title } = splitArticle(r.article);
  const tr = document.createElement("tr");

  cell(tr, formatDetectedAt(r.detected_at), "mono");

  const kind = cell(tr, `${KIND_ICON[r.kind] ?? "◦"} ${r.kind}`);
  kind.classList.add("kind", `kind-${r.kind}`);

  const article = document.createElement("td");
  const url = r.evidence?.title_url;
  if (url) {
    const a = document.createElement("a");
    a.textContent = title;
    a.href = url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    article.appendChild(a);
  } else {
    article.textContent = title;
  }
  const badge = document.createElement("span");
  badge.className = "badge inline";
  badge.textContent = wiki;
  article.appendChild(badge);
  tr.appendChild(article);

  cell(tr, r.distinct_eds === null ? "·" : String(r.distinct_eds), "mono num");
  cell(tr, r.peak_rate === null ? "·" : String(Math.round(r.peak_rate)), "mono num");

  // Evidence summary: what the gates saw plus the strongest human-readable clue.
  const g1 = r.evidence?.gate1;
  const cats = r.evidence?.categories ?? [];
  const comments = r.evidence?.sample_comments ?? [];
  const bits: string[] = [];
  if (g1?.anomaly !== undefined) bits.push(`${g1.anomaly.toFixed(1)}× baseline`);
  if (cats.length > 0) bits.push(cats[0]);
  else if (comments.length > 0) bits.push(comments[0].slice(0, 80));
  const ev = cell(tr, bits.join(" · ") || "—", "evidence");
  ev.title = `event #${r.id}`;

  return tr;
}

async function load(): Promise<void> {
  try {
    const res = await fetch(`${apiBase()}/v1/events?limit=200`);
    if (!res.ok) {
      setStatus("error", `ledger unavailable — HTTP ${res.status}`);
      return;
    }
    const data = (await res.json()) as { count: number; events: Receipt[] };
    if (!body) return;
    body.replaceChildren();

    if (data.events.length === 0) {
      const tr = document.createElement("tr");
      const td = cell(tr, "No detections yet — the gates have not confirmed a burst.");
      td.colSpan = 6;
      td.className = "empty";
      body.appendChild(tr);
      setStatus("idle", "ledger empty");
      if (meta) meta.textContent = "0 detections";
      return;
    }

    for (const r of data.events) body.appendChild(renderRow(r));
    setStatus("live", `${data.count} detections on record`);
    if (meta) meta.textContent = `${data.count} detections · newest first`;
  } catch (err) {
    setStatus("error", "could not reach the api");
    // eslint-disable-next-line no-console
    console.error(err);
  }
}

void load();
// Cheap refresh: the ledger grows a few times an hour, not a few times a second.
window.setInterval(() => void load(), 30_000);
