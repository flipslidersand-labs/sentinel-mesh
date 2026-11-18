// Persists the operator-supplied SENTINEL_API_TOKEN in the browser so every
// REST call to the collector can carry it as a Bearer token. Without this,
// enabling SENTINEL_API_TOKEN on the collector locks the bundled UI itself
// out with 401s (#120).
const STORAGE_KEY = "sentinel-mesh:api-token";

// Fallback used only when localStorage rejects a write. `null` means "defer to
// localStorage"; a string (even "") overrides whatever storage holds.
let memoryToken: string | null = null;

export function getToken(): string {
  if (memoryToken !== null) return memoryToken;
  try {
    return localStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    // localStorage can throw (private browsing, disabled storage, etc.) —
    // treat that the same as "no token configured".
    return "";
  }
}

/** Stores the token. Returns true when persisted to localStorage, false when
 * storage failed and the token is only kept in memory for this tab. */
export function setToken(token: string): boolean {
  const trimmed = token.trim();
  try {
    if (trimmed) {
      localStorage.setItem(STORAGE_KEY, trimmed);
    } else {
      localStorage.removeItem(STORAGE_KEY);
    }
    memoryToken = null;
    return true;
  } catch {
    memoryToken = trimmed;
    return false;
  }
}

export function hasToken(): boolean {
  return getToken() !== "";
}

/** Headers to merge into every `/api/*` request. Empty when no token is set,
 * matching the collector's own "unset = unauthenticated" behavior. */
export function authHeaders(): HeadersInit {
  const token = getToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}
