// Keeps the operator-supplied SENTINEL_API_TOKEN for the current browser tab so
// every REST call to the collector can carry it as a Bearer token. Without
// this, enabling SENTINEL_API_TOKEN on the collector locks the bundled UI
// itself out with 401s (#120).
//
// The token lives in sessionStorage, not localStorage: it is dropped when the
// tab or browser is closed instead of persisting on disk (#420).
const STORAGE_KEY = "sentinel-mesh:api-token";

// Fallback used only when sessionStorage rejects a write. `null` means "defer
// to sessionStorage"; a string (even "") overrides whatever storage holds.
let memoryToken: string | null = null;

// Earlier versions persisted the token in localStorage. Never read it back
// (that would keep the persistent copy alive); just delete it. Best effort:
// storage may be unavailable.
function removeLegacyToken(): void {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    // ignore: nothing more we can do if storage is blocked
  }
}

export function getToken(): string {
  removeLegacyToken();
  if (memoryToken !== null) return memoryToken;
  try {
    return sessionStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    // sessionStorage can throw (private browsing, disabled storage, etc.) —
    // treat that the same as "no token configured".
    return "";
  }
}

/** Stores the token for this tab. Returns true when saved to sessionStorage,
 * false when storage failed and the token is only kept in memory for this tab. */
export function setToken(token: string): boolean {
  const trimmed = token.trim();
  removeLegacyToken();
  try {
    if (trimmed) {
      sessionStorage.setItem(STORAGE_KEY, trimmed);
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
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
