import { describe, expect, it } from "vitest";
import { rangeMsFor, withinRange } from "./filters";

describe("rangeMsFor", () => {
  it("returns null for 'all'", () => {
    expect(rangeMsFor("all")).toBeNull();
  });

  it("returns the millisecond window for a known key", () => {
    expect(rangeMsFor("1h")).toBe(60 * 60 * 1000);
  });
});

describe("withinRange", () => {
  const now = new Date("2026-01-01T00:00:00Z").getTime();

  it("is always true when the window is null", () => {
    expect(withinRange("2000-01-01T00:00:00Z", null, now)).toBe(true);
  });

  it("is true for a timestamp inside the window", () => {
    const tenMinutesAgo = new Date(now - 10 * 60 * 1000).toISOString();
    expect(withinRange(tenMinutesAgo, 60 * 60 * 1000, now)).toBe(true);
  });

  it("is false for a timestamp outside the window", () => {
    const twoHoursAgo = new Date(now - 2 * 60 * 60 * 1000).toISOString();
    expect(withinRange(twoHoursAgo, 60 * 60 * 1000, now)).toBe(false);
  });

  it("keeps rows with an unparseable timestamp instead of hiding them", () => {
    expect(withinRange("not-a-date", 60 * 60 * 1000, now)).toBe(true);
  });
});
