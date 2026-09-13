// Persists the operator-supplied SENTINEL_API_TOKEN in the browser so every
// REST call to the collector can carry it as a Bearer token. Without this,
// enabling SENTINEL_API_TOKEN on the collector locks the bundled UI itself
// out with 401s (#120).
const STORAGE_KEY = "sentinel-mesh:api-token";

export function getToken(): string {
  try {
    return localStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    // localStorage can throw (private browsing, disabled storage, etc.) —
    // treat that the same as "no token configured".
    return "";
  }
}

export function setToken(token: string): void {
  const trimmed = token.trim();
  try {
    if (trimmed) {
      localStorage.setItem(STORAGE_KEY, trimmed);
    } else {
      localStorage.removeItem(STORAGE_KEY);
    }
  } catch {
    // Persistence best-effort only; the token still works for the current
    // page session via the in-memory value callers pass around.
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
