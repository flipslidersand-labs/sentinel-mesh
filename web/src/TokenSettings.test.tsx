// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getToken } from "./auth";
import { TokenSettings } from "./TokenSettings";

const KEY = "sentinel-mesh:api-token";

beforeEach(() => localStorage.clear());
afterEach(cleanup);

function openPanel() {
  fireEvent.click(screen.getByRole("button", { name: /API Token/ }));
  return screen.getByLabelText("SENTINEL_API_TOKEN") as HTMLInputElement;
}

describe("TokenSettings", () => {
  it("shows 'not set' and keeps the panel closed by default", () => {
    render(<TokenSettings />);
    const toggle = screen.getByRole("button", { name: "API Token: not set" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("shows 'set' and prefills the input when a token is already stored", () => {
    localStorage.setItem(KEY, "abc");
    render(<TokenSettings />);
    expect(screen.getByRole("button", { name: "API Token: set" })).toBeTruthy();
    expect(openPanel().value).toBe("abc");
  });

  it("toggles the panel open and closed", () => {
    render(<TokenSettings />);
    const toggle = screen.getByRole("button", { name: /API Token/ });
    fireEvent.click(toggle);
    expect(screen.getByRole("dialog")).toBeTruthy();
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    fireEvent.click(toggle);
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("saves a trimmed token, flips the label and shows 'Saved'", () => {
    render(<TokenSettings />);
    const input = openPanel();
    fireEvent.change(input, { target: { value: "  secret  " } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(localStorage.getItem(KEY)).toBe("secret");
    expect(getToken()).toBe("secret");
    expect(screen.getByRole("button", { name: "Saved" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "API Token: set" })).toBeTruthy();
  });

  it("saves on Enter", () => {
    render(<TokenSettings />);
    const input = openPanel();
    fireEvent.change(input, { target: { value: "via-enter" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(localStorage.getItem(KEY)).toBe("via-enter");
  });

  it("clears the stored token when saved blank", () => {
    localStorage.setItem(KEY, "old");
    render(<TokenSettings />);
    const input = openPanel();
    fireEvent.change(input, { target: { value: "   " } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(localStorage.getItem(KEY)).toBeNull();
    expect(screen.getByRole("button", { name: "API Token: not set" })).toBeTruthy();
  });
});
