import { useMemo, useState } from "react";
import { api, type Event } from "../api";
import { usePolling } from "../hooks/usePolling";
import {
  TIME_RANGES,
  rangeMsFor,
  withinRange,
  type RangeKey,
} from "../filters";

const EVENT_TYPES = ["exec", "tcp", "file"] as const;

function payloadText(payload: unknown): string {
  if (payload == null) return "";
  if (typeof payload === "string") return payload;
  try {
    return JSON.stringify(payload);
  } catch {
    return String(payload);
  }
}

export function Events() {
  const [node, setNode] = useState("");
  const [limit, setLimit] = useState(50);
  const [type, setType] = useState("");
  const [range, setRange] = useState<RangeKey>("all");

  // node + limit are applied server-side; type + time-range are client-side.
  const { data, error, loading } = usePolling<Event[]>(() =>
    api.events(node || undefined, limit),
  );

  const rangeMs = rangeMsFor(range);
  const filtered = useMemo(
    () =>
      (data ?? []).filter(
        (e) => (!type || e.type === type) && withinRange(e.timestamp, rangeMs),
      ),
    [data, type, rangeMs],
  );

  return (
    <>
      <div className="toolbar">
        <input
          placeholder="Filter by node ID"
          value={node}
          onChange={(e) => setNode(e.target.value)}
        />
        <select value={type} onChange={(e) => setType(e.target.value)}>
          <option value="">All types</option>
          {EVENT_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <select
          value={range}
          onChange={(e) => setRange(e.target.value as RangeKey)}
        >
          {TIME_RANGES.map((r) => (
            <option key={r.key} value={r.key}>
              {r.label}
            </option>
          ))}
        </select>
        <select
          value={limit}
          onChange={(e) => setLimit(Number(e.target.value))}
        >
          <option value={20}>20</option>
          <option value={50}>50</option>
          <option value={100}>100</option>
        </select>
      </div>

      {loading && <div className="status">Loading…</div>}
      {error && <div className="status error">{error}</div>}
      {!loading && !error && !filtered.length && (
        <div className="status">No events</div>
      )}
      {filtered.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Node</th>
              <th>Type</th>
              <th>Payload</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((e) => (
              <tr key={e.event_id}>
                <td className="mono">
                  {new Date(e.timestamp).toLocaleTimeString()}
                </td>
                <td className="mono">{e.node_id}</td>
                <td>
                  <span className="badge type">{e.type}</span>
                </td>
                <td className="payload">{payloadText(e.payload)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
