import { useState } from "react";
import { Nodes } from "./pages/Nodes";
import { Events } from "./pages/Events";
import { Alerts } from "./pages/Alerts";
import { Stats } from "./pages/Stats";
import "./App.css";

const TABS = ["Nodes", "Events", "Alerts", "Stats"] as const;
type Tab = (typeof TABS)[number];

export default function App() {
  const [tab, setTab] = useState<Tab>("Nodes");

  return (
    <div className="app">
      <header className="app-header">
        <span className="app-title">SentinelMesh</span>
        <nav className="tab-nav" role="tablist">
          {TABS.map((t) => (
            <button
              key={t}
              role="tab"
              aria-selected={tab === t}
              className={`tab-btn ${tab === t ? "active" : ""}`}
              onClick={() => setTab(t)}
            >
              {t}
            </button>
          ))}
        </nav>
      </header>
      <main className="app-main">
        {tab === "Nodes" && <Nodes />}
        {tab === "Events" && <Events />}
        {tab === "Alerts" && <Alerts />}
        {tab === "Stats" && <Stats />}
      </main>
    </div>
  );
}
