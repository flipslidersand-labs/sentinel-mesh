import { useMemo, useState } from "react";
import { api, asArray, type Alert } from "../api";
import { usePolling } from "../hooks/usePolling";
import { RangeSelect } from "../components/RangeSelect";
import { rangeMsFor, withinRange, type RangeKey } from "../filters";

const SEVERITIES = ["info", "warning", "critical"] as const;

// severity comes from the wire and may be null/undefined/non-string.
function sevText(severity: unknown): string {
  return typeof severity === "string" ? severity : "";
}

// Single mapping used by both the badge class and the severity filter.
function severityLevel(severity: unknown): (typeof SEVERITIES)[number] {
  const s = sevText(severity).toLowerCase();
  if (s === "critical" || s === "high" || s === "error") return "critical";
  if (s === "warning" || s === "warn" || s === "medium") return "warning";
  return "info";
}

function severityClass(severity: unknown): string {
  return `sev-${severityLevel(severity)}`;
}

export function Alerts() {
  const [node, setNode] = useState("");
  const [severity, setSeverity] = useState("");
  const [rule, setRule] = useState("");
  const [range, setRange] = useState<RangeKey>("all");

  // node is applied server-side; severity + rule + time-range are client-side.
  const { data, error, loading } = usePolling<Alert[]>(
    () => api.alerts(node || undefined),
    undefined,
    [node],
  );

  const rangeMs = rangeMsFor(range);
  const ruleQuery = rule.trim().toLowerCase();
  const filtered = useMemo(
    () =>
      asArray<Alert>(data).filter(
        (a) =>
          (!severity || severityLevel(a.severity) === severity) &&
          (!ruleQuery || (a.rule_id ?? "").toLowerCase().includes(ruleQuery)) &&
          withinRange(a.timestamp, rangeMs),
      ),
    [data, severity, ruleQuery, rangeMs],
  );

  return (
    <>
      <div className="toolbar">
        <input
          placeholder="Filter by node ID"
          value={node}
          onChange={(e) => setNode(e.target.value)}
        />
        <input
          placeholder="Filter by rule"
          value={rule}
          onChange={(e) => setRule(e.target.value)}
        />
        <select value={severity} onChange={(e) => setSeverity(e.target.value)}>
          <option value="">All severities</option>
          {SEVERITIES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <RangeSelect value={range} onChange={setRange} />
      </div>

      {loading && <div className="status">Loading…</div>}
      {error && <div className="status error">{error}</div>}
      {!loading && !error && !filtered.length && (
        <div className="status">No alerts</div>
      )}
      {filtered.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Node</th>
              <th>Severity</th>
              <th>Rule</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((a) => (
              <tr key={a.alert_id} className="alert-row">
                <td className="mono">
                  {new Date(a.timestamp).toLocaleString()}
                </td>
                <td className="mono">{a.node_id}</td>
                <td>
                  <span className={`badge ${severityClass(a.severity)}`}>
                    {sevText(a.severity)}
                  </span>
                </td>
                <td>
                  <span className="badge alert">{a.rule_id}</span>
                </td>
                <td>{a.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
