import { useEffect, useRef, useState } from "react";

export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs = 5000,
  // Values the fetcher depends on (e.g. filters). When any changes, the
  // in-flight request is discarded and a fresh fetch starts immediately.
  deps: readonly unknown[] = [],
): { data: T | null; error: string | null; loading: boolean } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Always holds the latest fetcher without triggering re-subscription
  const fetcherRef = useRef(fetcher);
  useEffect(() => {
    fetcherRef.current = fetcher;
  });

  useEffect(() => {
    let cancelled = false;
    // Guards against overlapping ticks: if the previous run() hasn't
    // resolved yet (e.g. the collector is hanging), skip this tick instead
    // of firing another fetch on top of it and letting requests pile up.
    let running = false;

    const run = async () => {
      if (running) return;
      running = true;
      try {
        const result = await fetcherRef.current();
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        running = false;
        if (!cancelled) setLoading(false);
      }
    };

    run();
    const id = setInterval(run, intervalMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, ...deps]);

  return { data, error, loading };
}
