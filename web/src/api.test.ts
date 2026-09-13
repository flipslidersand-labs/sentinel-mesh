import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

// vitest's default "node" environment has no localStorage; auth.ts reads it
// via getToken()/authHeaders(), so install a minimal in-memory stand-in.
function installFakeLocalStorage() {
  const store = new Map<string, string>();
  Object.defineProperty(globalThis, "localStorage", {
    value: {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => {
        store.set(key, value);
      },
      removeItem: (key: string) => {
        store.delete(key);
      },
      clear: () => store.clear(),
    },
    configurable: true,
  });
}

describe("api get() timeout", () => {
  beforeEach(() => {
    installFakeLocalStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("aborts and rejects with a timeout error when the collector hangs", async () => {
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      // Simulate a hung request: the returned promise never resolves on its
      // own, it only rejects once the AbortController we were given fires.
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          const err = new DOMException("aborted", "AbortError");
          reject(err);
        });
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const pending = api.stats();
    // Attach a rejection handler before advancing timers so the eventual
    // rejection is never "unhandled" during the fake-timer tick.
    const assertion = expect(pending).rejects.toThrow(/timed out/i);

    await vi.advanceTimersByTimeAsync(8000);
    await assertion;

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [, init] = fetchMock.mock.calls[0];
    expect(init?.signal?.aborted).toBe(true);
  });
});
