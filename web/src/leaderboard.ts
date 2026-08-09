/**
 * Leaderboard — PLAN.md Phase 3 (ELO) and Phase 5 (points).
 */
import { apiFetch } from "./api";

interface Row {
  handle: string;
  elo: number;
  points: number;
  settled_calls: number;
}

const body = document.getElementById("board-body") as HTMLTableSectionElement | null;
const statusEl = document.getElementById("status");
const statusText = document.getElementById("status-text");
let mode: "patrol" | "surge" = "patrol";

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

function cell(tr: HTMLTableRowElement, text: string, cls?: string): void {
  const td = document.createElement("td");
  td.textContent = text;
  if (cls) td.className = cls;
  tr.appendChild(td);
}

async function load(): Promise<void> {
  const res = await apiFetch(`/v1/leaderboard?mode=${mode}&limit=50`);
  if (!res.ok) {
    setStatus("error", `leaderboard unavailable (HTTP ${res.status})`);
    return;
  }
  const data = (await res.json()) as { players: Row[] };
  if (!body) return;
  body.replaceChildren();

  if (data.players.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.className = "empty";
    td.textContent = "No players yet — be the first to patrol.";
    tr.appendChild(td);
    body.appendChild(tr);
    setStatus("idle", "no players yet");
    return;
  }

  data.players.forEach((p, i) => {
    const tr = document.createElement("tr");
    cell(tr, String(i + 1), "mono num");
    cell(tr, p.handle);
    cell(tr, String(Math.round(p.elo)), "mono num");
    cell(tr, String(p.points), "mono num");
    cell(tr, String(p.settled_calls), "mono num");
    body.appendChild(tr);
  });
  setStatus("live", `${data.players.length} players · ${mode}`);
}

/** Reflect the active mode in both the class and `aria-pressed`. */
function paintModes(): void {
  for (const id of ["mode-patrol", "mode-surge"] as const) {
    const btn = document.getElementById(id);
    if (!btn) continue;
    const active = btn.dataset.mode === mode;
    btn.classList.toggle("primary", active);
    btn.setAttribute("aria-pressed", String(active));
  }
}

for (const id of ["mode-patrol", "mode-surge"]) {
  document.getElementById(id)?.addEventListener("click", (e) => {
    const btn = e.currentTarget as HTMLElement;
    mode = (btn.dataset.mode as "patrol" | "surge") ?? "patrol";
    paintModes();
    void load();
  });
}
paintModes();
void load();
window.setInterval(() => void load(), 20_000);
