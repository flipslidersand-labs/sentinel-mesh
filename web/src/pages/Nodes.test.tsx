// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, type NodeInfo } from "../api";
import { Nodes } from "./Nodes";

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: { ...actual.api, nodes: vi.fn() } };
});

const nodesMock = vi.mocked(api.nodes);

afterEach(() => {
  cleanup();
  nodesMock.mockReset();
});

describe("Nodes page", () => {
  it("shows loading before the first response", () => {
    nodesMock.mockReturnValue(new Promise(() => {}));
    render(<Nodes />);
    expect(screen.getByText("Loading…")).toBeTruthy();
  });

  it("shows the error message when the request fails", async () => {
    nodesMock.mockRejectedValue(new Error("collector down"));
    render(<Nodes />);
    expect(await screen.findByText(/collector down/)).toBeTruthy();
  });

  it("shows the empty state when no nodes are registered", async () => {
    nodesMock.mockResolvedValue([]);
    render(<Nodes />);
    expect(await screen.findByText("No nodes registered")).toBeTruthy();
  });

  it("renders one row per node with status badge", async () => {
    nodesMock.mockResolvedValue([
      {
        node_id: "node-1",
        hostname: "host-a",
        region: "jp-east",
        status: "active",
        last_seen: new Date().toISOString(),
      },
      {
        node_id: "node-2",
        hostname: "host-b",
        region: "jp-west",
        status: "inactive",
        last_seen: new Date(Date.now() - 2 * 3600 * 1000).toISOString(),
      },
    ] as unknown as NodeInfo[]);
    render(<Nodes />);

    expect(await screen.findByText("node-1")).toBeTruthy();
    expect(screen.getByText("host-b")).toBeTruthy();
    expect(screen.getAllByRole("row")).toHaveLength(3); // header + 2
    expect(screen.getByText("active").className).toContain("badge active");
    expect(screen.getByText("inactive").className).toContain("badge inactive");
    expect(screen.getByText("2h ago")).toBeTruthy();
  });
});
