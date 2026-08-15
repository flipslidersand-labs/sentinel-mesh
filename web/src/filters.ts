// Shared client-side filtering helpers for the Events and Alerts views.

export type RangeKey = "all" | "1h" | "24h" | "7d";

export const TIME_RANGES: {
  key: RangeKey;
  label: string;
  ms: number | null;
}[] = [
  { key: "all", label: "All time", ms: null },
  { key: "1h", label: "Last 1h", ms: 60 * 60 * 1000 },
  { key: "24h", label: "Last 24h", ms: 24 * 60 * 60 * 1000 },
  { key: "7d", label: "Last 7d", ms: 7 * 24 * 60 * 60 * 1000 },
];

export function rangeMsFor(key: RangeKey): number | null {
  return TIME_RANGES.find((r) => r.key === key)?.ms ?? null;
}

// withinRange returns true when the ISO timestamp falls inside the window.
// A null window means "no time filter". Unparseable timestamps are kept so a
// bad value never silently hides a row.
export function withinRange(
  timestamp: string,
  ms: number | null,
  now: number = Date.now(),
): boolean {
  if (ms === null) return true;
  const t = new Date(timestamp).getTime();
  if (Number.isNaN(t)) return true;
  return now - t <= ms;
}
