import { beforeEach, describe, expect, it } from "vitest";
import { authHeaders, getToken, hasToken, setToken } from "./auth";

const KEY = "sentinel-mesh:api-token";

// vitest's default "node" environment has no localStorage/sessionStorage, so
// this test installs minimal in-memory stand-ins before each case.
function makeFakeStorage(): Storage {
  const store = new Map<string, string>();
  return {
    get length() {
      return store.size;
    },
    clear: () => store.clear(),
    getItem: (key: string) => store.get(key) ?? null,
    key: (index: number) => Array.from(store.keys())[index] ?? null,
    removeItem: (key: string) => {
      store.delete(key);
    },
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
  };
}

function install(name: "localStorage" | "sessionStorage", value: unknown) {
  Object.defineProperty(globalThis, name, { value, configurable: true });
}

function installFakeStorages() {
  install("localStorage", makeFakeStorage());
  install("sessionStorage", makeFakeStorage());
}

const blocked = {
  getItem: () => {
    throw new Error("blocked");
  },
  setItem: () => {
    throw new Error("blocked");
  },
  removeItem: () => {
    throw new Error("blocked");
  },
} as unknown as Storage;

describe("auth token storage", () => {
  beforeEach(() => {
    installFakeStorages();
    // Reset the module-level in-memory fallback left by earlier cases.
    setToken("");
  });

  it("has no token by default", () => {
    expect(getToken()).toBe("");
    expect(hasToken()).toBe(false);
    expect(authHeaders()).toEqual({});
  });

  it("keeps a saved token for the tab and exposes it as a Bearer header", () => {
    setToken("abc123");
    expect(getToken()).toBe("abc123");
    expect(hasToken()).toBe(true);
    expect(authHeaders()).toEqual({ Authorization: "Bearer abc123" });
  });

  it("stores the token in sessionStorage and never writes localStorage", () => {
    setToken("abc123");
    expect(sessionStorage.getItem(KEY)).toBe("abc123");
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("ignores and deletes a token left in localStorage by an older version", () => {
    localStorage.setItem(KEY, "legacy");
    expect(getToken()).toBe("");
    expect(hasToken()).toBe(false);
    expect(localStorage.getItem(KEY)).toBeNull();
    // Not migrated into sessionStorage either.
    expect(sessionStorage.getItem(KEY)).toBeNull();
  });

  it("deletes a legacy localStorage token when a new token is saved", () => {
    localStorage.setItem(KEY, "legacy");
    setToken("fresh");
    expect(localStorage.getItem(KEY)).toBeNull();
    expect(getToken()).toBe("fresh");
  });

  it("deletes a legacy localStorage token when the token is cleared", () => {
    localStorage.setItem(KEY, "legacy");
    setToken("");
    expect(localStorage.getItem(KEY)).toBeNull();
    expect(getToken()).toBe("");
  });

  it("trims whitespace around the token", () => {
    setToken("  padded-token  ");
    expect(getToken()).toBe("padded-token");
  });

  it("clears the token when set to an empty string", () => {
    setToken("abc123");
    setToken("");
    expect(getToken()).toBe("");
    expect(hasToken()).toBe(false);
  });

  it("treats a sessionStorage failure as no token instead of throwing", () => {
    install("sessionStorage", blocked);
    expect(getToken()).toBe("");
    expect(() => setToken("x")).not.toThrow();
  });

  it("does not throw when localStorage is blocked", () => {
    install("localStorage", blocked);
    expect(getToken()).toBe("");
    expect(setToken("abc123")).toBe(true);
    expect(getToken()).toBe("abc123");
  });

  it("falls back to an in-memory token when sessionStorage rejects writes", () => {
    const token = "x".repeat(32);
    const failing = {
      getItem: () => null,
      setItem: () => {
        throw new Error("QuotaExceededError");
      },
      removeItem: () => {
        throw new Error("denied");
      },
    } as unknown as Storage;
    install("sessionStorage", failing);
    expect(setToken(token)).toBe(false);
    expect(getToken()).toBe(token);
    expect(authHeaders()).toEqual({ Authorization: `Bearer ${token}` });
    expect(localStorage.getItem(KEY)).toBeNull();
    expect(setToken("")).toBe(false);
    expect(getToken()).toBe("");
    // Recover: a successful write returns to sessionStorage as the source of truth.
    installFakeStorages();
    expect(setToken(token)).toBe(true);
    expect(getToken()).toBe(token);
  });
});
