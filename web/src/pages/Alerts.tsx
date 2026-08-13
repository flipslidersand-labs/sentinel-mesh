import { useState } from "react";
import { api, type Alert } from "../api";
import { usePolling } from "../hooks/usePolling";

export function Alerts() {
  const [node, setNode] = useState("");

  const { data, error, loading } = usePolling<Alert[]>(() =>
    api.alerts(node || undefined),
  );

  return (
    <>
      <div className="toolbar">
        <input
          placeholder="Filter by node ID"
          value={node}
          onChange={(e) => setNode(e.target.value)}
        />
      </div>

      {loading && <div className="status">Loading…</div>}
      {error && <div className="status error">{error}</div>}
      {!loading && !error && !data?.length && (
        <div className="status">No alerts</div>
      )}
      {data && data.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Node</th>
              <th>Rule</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {data.map((a) => (
              <tr key={a.id} className="alert-row">
                <td className="mono">
                  {new Date(a.triggered_at).toLocaleString()}
                </td>
                <td className="mono">{a.node_id}</td>
                <td>
                  <span className="badge alert">{a.rule_name}</span>
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
