/**
 * Pulse web — the live wall.
 *
 * Phase 0 scope: mount the shell and report that the static build shipped.
 * Phase 1 replaces `boot()` with an EventSource against the api's `/v1/live`.
 */

const statusEl = document.getElementById("status");
const statusText = document.getElementById("status-text");
const wallMeta = document.getElementById("wall-meta");

function setStatus(state: "idle" | "live" | "error", text: string): void {
  statusEl?.setAttribute("data-state", state);
  if (statusText) statusText.textContent = text;
}

function boot(): void {
  setStatus("idle", "phase 0 — skeleton deployed");
  if (wallMeta) wallMeta.textContent = "awaiting firehose";
  // eslint-disable-next-line no-console
  console.info("pulse web booted; SSE wiring lands in phase 1");
}

boot();
