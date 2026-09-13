import { beforeEach, describe, expect, it } from "vitest";
import { authHeaders, getToken, hasToken, setToken } from "./auth";

// vitest's default "node" environment has no localStorage, so this test
// installs a minimal in-memory stand-in before each case.
function installFakeLocalStorage() {
  const store = new Map<string, string>();
  const fake: Storage = {
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
  Object.defineProperty(globalThis, "localStorage", {
    value: fake,
    configurable: true,
  });
}

describe("auth token storage", () => {
  beforeEach(() => {
    installFakeLocalStorage();
  });

  it("has no token by default", () => {
    expect(getToken()).toBe("");
    expect(hasToken()).toBe(false);
    expect(authHeaders()).toEqual({});
  });

  it("persists a saved token and exposes it as a Bearer header", () => {
    setToken("abc123");
    expect(getToken()).toBe("abc123");
    expect(hasToken()).toBe(true);
    expect(authHeaders()).toEqual({ Authorization: "Bearer abc123" });
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

  it("treats a localStorage failure as no token instead of throwing", () => {
    Object.defineProperty(globalThis, "localStorage", {
      value: {
        getItem: () => {
          throw new Error("blocked");
        },
        setItem: () => {
          throw new Error("blocked");
        },
        removeItem: () => {
          throw new Error("blocked");
        },
      },
      configurable: true,
    });

    expect(getToken()).toBe("");
    expect(() => setToken("x")).not.toThrow();
  });
});
