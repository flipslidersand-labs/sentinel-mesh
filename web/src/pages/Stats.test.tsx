// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, type Stats as StatsType } from "../api";
import { Stats } from "./Stats";

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: { ...actual.api, stats: vi.fn() } };
});

const statsMock = vi.mocked(api.stats);

afterEach(() => {
  cleanup();
  statsMock.mockReset();
});

describe("Stats page", () => {
  it("shows loading before the first response", () => {
    statsMock.mockReturnValue(new Promise(() => {}));
    render(<Stats />);
    expect(screen.getByText("Loading…")).toBeTruthy();
  });

  it("shows the error message when the request fails", async () => {
    statsMock.mockRejectedValue(new Error("collector down"));
    render(<Stats />);
    expect(await screen.findByText(/collector down/)).toBeTruthy();
  });

  it("shows the empty state when there are no counts", async () => {
    statsMock.mockResolvedValue({} as StatsType);
    render(<Stats />);
    expect(await screen.findByText("No data")).toBeTruthy();
  });

  it("shows the total and one card per type, largest first, with percentages", async () => {
    statsMock.mockResolvedValue({ exec: 25, tcp: 1000, file: 75 } as unknown as StatsType);
    const { container } = render(<Stats />);

    expect(await screen.findByText("Total Events")).toBeTruthy();
    expect(screen.getByText((1100).toLocaleString())).toBeTruthy();
    const labels = Array.from(container.querySelectorAll(".stat-card:not(.total) .stat-label")).map(
      (n) => n.textContent,
    );
    expect(labels).toEqual(["tcp", "file", "exec"]);
    expect(screen.getByText("91%")).toBeTruthy(); // 1000/1100
    expect(screen.getByText("7%")).toBeTruthy(); // 75/1100
    expect(screen.getByText("2%")).toBeTruthy(); // 25/1100
    const fills = container.querySelectorAll<HTMLElement>(".stat-bar-fill");
    expect(fills[0].style.width).toBe("91%");
  });
});
