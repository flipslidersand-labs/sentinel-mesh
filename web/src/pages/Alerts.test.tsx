// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, type Alert } from "../api";
import { Alerts } from "./Alerts";

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: { ...actual.api, alerts: vi.fn() } };
});

const alertsMock = vi.mocked(api.alerts);

afterEach(() => {
  cleanup();
  alertsMock.mockReset();
});

function alert(over: Partial<Alert>): Alert {
  return {
    alert_id: "a-1",
    rule_id: "rule-a",
    node_id: "node-1",
    event_id: "e-1",
    timestamp: new Date().toISOString(),
    message: "msg",
    severity: "info",
    ...over,
  };
}

describe("Alerts page", () => {
  it("shows loading before the first response", () => {
    alertsMock.mockReturnValue(new Promise(() => {}));
    render(<Alerts />);
    expect(screen.getByText("Loading…")).toBeTruthy();
  });

  it("shows the error message when the request fails", async () => {
    alertsMock.mockRejectedValue(new Error("collector down"));
    render(<Alerts />);
    expect(await screen.findByText(/collector down/)).toBeTruthy();
  });

  it("shows the empty state when there are no alerts", async () => {
    alertsMock.mockResolvedValue([]);
    render(<Alerts />);
    expect(await screen.findByText("No alerts")).toBeTruthy();
  });

  it("renders one row per alert with severity badge class", async () => {
    alertsMock.mockResolvedValue([
      alert({ alert_id: "a-1", message: "first", severity: "critical" }),
      alert({ alert_id: "a-2", message: "second", severity: "warning" }),
      alert({ alert_id: "a-3", message: "third", severity: "info" }),
    ]);
    render(<Alerts />);

    expect(await screen.findByText("first")).toBeTruthy();
    const table = screen.getByRole("table");
    expect(within(table).getAllByRole("row")).toHaveLength(4); // header + 3
    expect(within(table).getByText("critical").className).toContain("sev-critical");
    expect(within(table).getByText("warning").className).toContain("sev-warning");
    expect(within(table).getByText("info").className).toContain("sev-info");
  });

  it("filters by severity and rule on the client", async () => {
    alertsMock.mockResolvedValue([
      alert({ alert_id: "a-1", rule_id: "rule-a", message: "m1", severity: "critical" }),
      alert({ alert_id: "a-2", rule_id: "rule-b", message: "m2", severity: "critical" }),
      alert({ alert_id: "a-3", rule_id: "rule-a", message: "m3", severity: "info" }),
    ]);
    render(<Alerts />);
    await screen.findByText("m1");

    const [severity] = screen.getAllByRole("combobox");
    fireEvent.change(severity, { target: { value: "critical" } });
    expect(screen.queryByText("m3")).toBeNull();
    expect(screen.getByText("m2")).toBeTruthy();

    fireEvent.change(screen.getByPlaceholderText("Filter by rule"), {
      target: { value: "RULE-B" },
    });
    expect(screen.queryByText("m1")).toBeNull();
    expect(screen.getByText("m2")).toBeTruthy();
  });
});
