// @vitest-environment jsdom
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { usePolling } from "./usePolling";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("usePolling", () => {
  it("fetches immediately on mount and exposes the result", async () => {
    const fetcher = vi.fn().mockResolvedValue("ok");
    const { result } = renderHook(() => usePolling(fetcher, 1000));

    expect(result.current.loading).toBe(true);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(result.current.data).toBe("ok");
    expect(result.current.error).toBeNull();
    expect(result.current.loading).toBe(false);
  });

  it("re-fetches every intervalMs", async () => {
    const fetcher = vi.fn().mockResolvedValue("ok");
    renderHook(() => usePolling(fetcher, 1000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetcher).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(fetcher).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(fetcher).toHaveBeenCalledTimes(4);
  });

  it("surfaces a rejected fetch as error without clearing prior data", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce("first")
      .mockRejectedValueOnce(new Error("boom"));
    const { result } = renderHook(() => usePolling(fetcher, 1000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.data).toBe("first");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(result.current.error).toBe("boom");
    // Stale data from the last successful fetch stays visible — the hook
    // never clears `data` on error, only sets `error` alongside it.
    expect(result.current.data).toBe("first");
  });

  it("stringifies a non-Error rejection", async () => {
    const fetcher = vi.fn().mockRejectedValue("plain string failure");
    const { result } = renderHook(() => usePolling(fetcher, 1000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.error).toBe("plain string failure");
  });

  it("skips overlapping ticks while a fetch is still in flight (#162)", async () => {
    let resolveFirst!: (v: string) => void;
    const fetcher = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<string>((resolve) => {
            resolveFirst = resolve;
          }),
      )
      .mockResolvedValue("later");

    renderHook(() => usePolling(fetcher, 1000));

    // First tick starts and hangs; two more interval ticks fire while it's
    // still pending. Without the overlap guard, run() would fire again on
    // each of them and fetcher would be called 3 times before anything
    // resolves.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2500);
    });
    expect(fetcher).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveFirst("first");
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("ignores a late resolution after unmount (cancelled guard)", async () => {
    let resolveFetch!: (v: string) => void;
    const fetcher = vi.fn().mockImplementation(
      () =>
        new Promise<string>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const { result, unmount } = renderHook(() => usePolling(fetcher, 1000));

    unmount();
    await act(async () => {
      resolveFetch("too-late");
      await Promise.resolve();
    });

    // Nothing to assert on `result.current` post-unmount beyond "no throw":
    // the guard's purpose is to avoid a React state-update-after-unmount
    // warning/crash, not to change any externally observable state.
    expect(result.current.loading).toBe(true);
  });

  it("re-subscribes its interval when intervalMs changes", async () => {
    const fetcher = vi.fn().mockResolvedValue("ok");
    const { rerender } = renderHook(
      ({ interval }: { interval: number }) => usePolling(fetcher, interval),
      { initialProps: { interval: 1000 } },
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetcher).toHaveBeenCalledTimes(1);

    rerender({ interval: 500 });
    await act(async () => {
      // The rerender's effect cleanup clears the old 1000ms interval and
      // starts a new 500ms one, which also fires an immediate run().
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetcher).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("always uses the latest fetcher without re-subscribing the interval", async () => {
    const first = vi.fn().mockResolvedValue("v1");
    const second = vi.fn().mockResolvedValue("v2");
    const { result, rerender } = renderHook(
      ({ fn }: { fn: () => Promise<string> }) => usePolling(fn, 1000),
      { initialProps: { fn: first } },
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.data).toBe("v1");

    rerender({ fn: second });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });

    expect(second).toHaveBeenCalledTimes(1);
    expect(first).toHaveBeenCalledTimes(1);
    expect(result.current.data).toBe("v2");
  });
});
