import { api, type NodeInfo } from "../api";
import { usePolling } from "../hooks/usePolling";

function timeSince(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export function Nodes() {
  const { data, error, loading } = usePolling<NodeInfo[]>(api.nodes);

  if (loading) return <div className="status">Loading…</div>;
  if (error) return <div className="status error">{error}</div>;
  if (!data?.length) return <div className="status">No nodes registered</div>;

  return (
    <table>
      <thead>
        <tr>
          <th>Node ID</th>
          <th>Status</th>
          <th>Last Seen</th>
        </tr>
      </thead>
      <tbody>
        {data.map((n) => (
          <tr key={n.node_id}>
            <td className="mono">{n.node_id}</td>
            <td>
              <span className={`badge ${n.active ? "active" : "inactive"}`}>
                {n.active ? "active" : "inactive"}
              </span>
            </td>
            <td>{timeSince(n.last_seen)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
