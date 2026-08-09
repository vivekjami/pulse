/**
 * Where the api lives, shared by the wall and the receipts page.
 *
 * Zerops bakes `VITE_API_BASE` at build time from the api service's own
 * subdomain (`${api_zeropsSubdomain}`). If that reference ever fails to resolve
 * we derive it from our own hostname instead, which also makes `npm run dev`
 * work against a locally-run api.
 */
export function apiBase(): string {
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

/** Event-type glyphs. Kept text-only so there are no asset dependencies. */
export const KIND_ICON: Record<string, string> = {
  death: "†",
  disaster: "▲",
  sports: "◆",
  political: "§",
  unclassified: "◦",
};

/** Split "{wiki}:{title}" back into its parts. Titles may contain colons. */
export function splitArticle(article: string): { wiki: string; title: string } {
  const i = article.indexOf(":");
  if (i < 0) return { wiki: "", title: article };
  return { wiki: article.slice(0, i), title: article.slice(i + 1) };
}

/** Absolute then relative time, e.g. "08:42:07Z · 3m ago". */
export function formatDetectedAt(iso: string): string {
  const then = new Date(iso);
  if (Number.isNaN(then.getTime())) return iso;
  const secs = Math.max(0, Math.round((Date.now() - then.getTime()) / 1000));
  const rel =
    secs < 60
      ? `${secs}s ago`
      : secs < 3600
        ? `${Math.floor(secs / 60)}m ago`
        : `${Math.floor(secs / 3600)}h ago`;
  return `${then.toISOString().slice(11, 19)}Z · ${rel}`;
}

/**
 * Fetch against the api carrying the signed player cookie.
 *
 * `credentials: "include"` is required because the SPA and the api are on
 * different origins; the api mirrors the request origin and sets
 * `Access-Control-Allow-Credentials`, which a wildcard origin cannot do.
 */
export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${apiBase()}${path}`, {
    ...init,
    credentials: "include",
    headers: { "Content-Type": "application/json", ...(init.headers ?? {}) },
  });
}

export interface Player {
  id: number;
  handle: string;
  elo: number;
  points: number;
}

/** Who am I? `null` when there is no valid cookie yet. */
export async function whoami(): Promise<Player | null> {
  const res = await apiFetch("/v1/me");
  return res.ok ? ((await res.json()) as Player) : null;
}

/** Claim a handle and receive the signed cookie. */
export async function join(handle: string): Promise<Player | null> {
  const res = await apiFetch("/v1/players", {
    method: "POST",
    body: JSON.stringify({ handle }),
  });
  return res.ok ? ((await res.json()) as Player) : null;
}

/** Split "{wiki}:{title}" and render it for display. */
export function articleLabel(article: string): string {
  const { wiki, title } = splitArticle(article);
  return wiki ? `${title} (${wiki})` : title;
}
