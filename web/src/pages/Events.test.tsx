// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, type Event } from "../api";
import { Events } from "./Events";

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: { ...actual.api, events: vi.fn() } };
});

const eventsMock = vi.mocked(api.events);

afterEach(() => {
  cleanup();
  eventsMock.mockReset();
});

function event(over: Partial<Event>): Event {
  return {
    event_id: "e-1",
    node_id: "node-1",
    timestamp: new Date().toISOString(),
    type: "exec",
    payload: {},
    ...over,
  };
}

describe("Events page", () => {
  it("shows loading before the first response", () => {
    eventsMock.mockReturnValue(new Promise(() => {}));
    render(<Events />);
    expect(screen.getByText("Loading…")).toBeTruthy();
  });

  it("shows the error message when the request fails", async () => {
    eventsMock.mockRejectedValue(new Error("collector down"));
    render(<Events />);
    expect(await screen.findByText(/collector down/)).toBeTruthy();
  });

  it("shows the empty state when there are no events", async () => {
    eventsMock.mockResolvedValue([]);
    render(<Events />);
    expect(await screen.findByText("No events")).toBeTruthy();
  });

  it("renders one row per event and serializes object payloads", async () => {
    eventsMock.mockResolvedValue([
      event({ event_id: "e-1", payload: { comm: "bash" } }),
      event({ event_id: "e-2", type: "tcp", payload: "raw-string" }),
      event({ event_id: "e-3", type: "file", payload: null }),
    ]);
    render(<Events />);

    expect(await screen.findByText('{"comm":"bash"}')).toBeTruthy();
    const table = screen.getByRole("table");
    expect(within(table).getAllByRole("row")).toHaveLength(4); // header + 3
    expect(within(table).getByText("raw-string")).toBeTruthy();
  });

  it("filters by type on the client", async () => {
    eventsMock.mockResolvedValue([
      event({ event_id: "e-1", node_id: "n-exec", type: "exec" }),
      event({ event_id: "e-2", node_id: "n-tcp", type: "tcp" }),
    ]);
    render(<Events />);
    await screen.findByText("n-exec");

    const [type] = screen.getAllByRole("combobox");
    fireEvent.change(type, { target: { value: "tcp" } });
    expect(screen.queryByText("n-exec")).toBeNull();
    expect(screen.getByText("n-tcp")).toBeTruthy();
  });

  it("requests the default limit of 50 without a node filter", async () => {
    eventsMock.mockResolvedValue([]);
    render(<Events />);
    await screen.findByText("No events");
    expect(eventsMock).toHaveBeenCalledWith(undefined, 50);
  });
});
