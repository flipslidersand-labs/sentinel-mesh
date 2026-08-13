import { api, type Stats as StatsType } from "../api";
import { usePolling } from "../hooks/usePolling";

export function Stats() {
  const { data, error, loading } = usePolling<StatsType>(() => api.stats());

  if (loading) return <div className="status">Loading…</div>;
  if (error) return <div className="status error">{error}</div>;
  if (!data || !Object.keys(data).length)
    return <div className="status">No data</div>;

  const total = Object.values(data).reduce((s, n) => s + n, 0);
  const sorted = Object.entries(data).sort(([, a], [, b]) => b - a);

  return (
    <div className="stats-grid">
      <div className="stat-card total">
        <div className="stat-value">{total.toLocaleString()}</div>
        <div className="stat-label">Total Events</div>
      </div>
      {sorted.map(([type, count]) => {
        const pct = total > 0 ? Math.round((count / total) * 100) : 0;
        return (
          <div key={type} className="stat-card">
            <div className="stat-value">{count.toLocaleString()}</div>
            <div className="stat-label">{type}</div>
            <div className="stat-bar">
              <div className="stat-bar-fill" style={{ width: `${pct}%` }} />
            </div>
            <div className="stat-pct">{pct}%</div>
          </div>
        );
      })}
    </div>
  );
}
