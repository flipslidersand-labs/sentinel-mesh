export interface NodeInfo {
  node_id: string;
  hostname: string;
  ip: string;
  version: string;
  region: string;
  last_seen: string;
  status: string; // "active" | "inactive"
}

export interface Event {
  event_id: string;
  node_id: string;
  timestamp: string;
  type: string; // "exec" | "tcp" | "file"
  payload: unknown; // raw event payload (JSON object)
}

export interface Alert {
  alert_id: string;
  rule_id: string;
  node_id: string;
  event_id: string;
  timestamp: string;
  message: string;
  severity: string; // "info" | "warning" | "critical"
}

export type Stats = Record<string, number>;

// The REST API is documented to return an array/object, but a `200` response
// isn't a shape guarantee — Go nil slices/maps serialize to `null`, and any
// future proxy/aggregator layer could return something else entirely. `get<T>`
// casts the parsed JSON to `T` with no runtime check, so callers must guard
// the shape themselves before array/object methods, or a mismatch throws
// synchronously during render (uncaught, since usePolling only wraps the
// fetch itself) and white-screens the tab (#80).
export function asArray<T>(v: unknown): T[] {
  return Array.isArray(v) ? (v as T[]) : [];
}

export function asRecord(v: unknown): Record<string, number> {
  return v !== null && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, number>)
    : {};
}

import { authHeaders } from "./auth";

const BASE = "/api";
const TIMEOUT_MS = 8000;

async function get<T>(path: string): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(BASE + path, {
      headers: authHeaders(),
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      throw new Error(`Request timed out after ${TIMEOUT_MS}ms: ${path}`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

export const api = {
  nodes: () => get<NodeInfo[]>("/nodes"),
  events: (node?: string, limit = 100) => {
    const q = new URLSearchParams();
    if (node) q.set("node", node);
    q.set("limit", String(limit));
    return get<Event[]>(`/events?${q}`);
  },
  alerts: (node?: string, limit = 100) => {
    const q = new URLSearchParams();
    if (node) q.set("node", node);
    q.set("limit", String(limit));
    return get<Alert[]>(`/alerts?${q}`);
  },
  stats: () => get<Stats>("/stats"),
};
