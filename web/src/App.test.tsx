// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";

// Pages poll the API; App's job is only tab routing, so stub them out.
vi.mock("./pages/Nodes", () => ({ Nodes: () => <div>nodes-page</div> }));
vi.mock("./pages/Events", () => ({ Events: () => <div>events-page</div> }));
vi.mock("./pages/Alerts", () => ({ Alerts: () => <div>alerts-page</div> }));
vi.mock("./pages/Stats", () => ({ Stats: () => <div>stats-page</div> }));

afterEach(cleanup);

describe("App", () => {
  it("renders the title, four tabs and the token widget, with Nodes selected", () => {
    render(<App />);
    expect(screen.getByText("SentinelMesh")).toBeTruthy();
    expect(screen.getAllByRole("tab").map((t) => t.textContent)).toEqual([
      "Nodes",
      "Events",
      "Alerts",
      "Stats",
    ]);
    expect(screen.getByRole("tab", { name: "Nodes" }).getAttribute("aria-selected")).toBe("true");
    expect(screen.getByText("nodes-page")).toBeTruthy();
    expect(screen.getByRole("button", { name: /API Token/ })).toBeTruthy();
  });

  it.each([
    ["Events", "events-page"],
    ["Alerts", "alerts-page"],
    ["Stats", "stats-page"],
  ])("switches to the %s page when its tab is clicked", (tab, page) => {
    render(<App />);
    fireEvent.click(screen.getByRole("tab", { name: tab }));

    expect(screen.getByText(page)).toBeTruthy();
    expect(screen.queryByText("nodes-page")).toBeNull();
    expect(screen.getByRole("tab", { name: tab }).getAttribute("aria-selected")).toBe("true");
    expect(screen.getByRole("tab", { name: "Nodes" }).getAttribute("aria-selected")).toBe("false");
  });

  it("shows only one page at a time and can return to Nodes", () => {
    render(<App />);
    fireEvent.click(screen.getByRole("tab", { name: "Alerts" }));
    fireEvent.click(screen.getByRole("tab", { name: "Nodes" }));
    expect(screen.getByText("nodes-page")).toBeTruthy();
    expect(screen.queryByText("alerts-page")).toBeNull();
  });
});
