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

const BASE = "/api";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
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
