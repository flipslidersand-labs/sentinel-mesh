import { useState } from "react";
import { getToken, hasToken, setToken } from "./auth";

/** Header widget that lets an operator paste the collector's
 * `SENTINEL_API_TOKEN` so the UI's own fetch calls carry it. Without this
 * the dashboard has no way to authenticate against a collector that has
 * the token configured (#120). */
export function TokenSettings() {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(() => getToken());
  const [saved, setSaved] = useState(false);
  const [memoryOnly, setMemoryOnly] = useState(false);

  function save() {
    setMemoryOnly(!setToken(draft));
    setSaved(true);
    setTimeout(() => setSaved(false), 1500);
  }

  return (
    <div className="token-settings">
      <button
        type="button"
        className="token-settings-toggle"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {hasToken() ? "API Token: set" : "API Token: not set"}
      </button>
      {open && (
        <div className="token-settings-panel" role="dialog" aria-label="API token">
          <label htmlFor="api-token-input">SENTINEL_API_TOKEN</label>
          <input
            id="api-token-input"
            type="password"
            autoComplete="off"
            spellCheck={false}
            placeholder="Bearer token"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") save();
            }}
          />
          <button type="button" onClick={save}>
            {saved ? (memoryOnly ? "This tab only" : "Saved") : "Save"}
          </button>
          {memoryOnly && (
            <p className="token-settings-hint" role="alert">
              Could not save to localStorage; the token is valid for this tab
              only and will be lost on reload.
            </p>
          )}
          <p className="token-settings-hint">
            Stored in this browser's localStorage only and sent as
            <code> Authorization: Bearer &lt;token&gt;</code> on every API
            request. Leave blank to clear it.
          </p>
        </div>
      )}
    </div>
  );
}
