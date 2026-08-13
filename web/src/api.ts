export interface NodeInfo {
  node_id: string;
  last_seen: string;
  active: boolean;
}

export interface Event {
  id: string;
  node_id: string;
  event_type: string;
  payload: string;
  timestamp: string;
}

export interface Alert {
  id: string;
  node_id: string;
  rule_name: string;
  message: string;
  triggered_at: string;
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
