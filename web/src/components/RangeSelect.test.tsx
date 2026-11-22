// @vitest-environment jsdom
import { cleanup, render, screen, fireEvent } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RangeSelect } from "./RangeSelect";
import { TIME_RANGES } from "../filters";

afterEach(cleanup);

describe("RangeSelect", () => {
  it("renders all ranges and reports changes", () => {
    const onChange = vi.fn();
    render(<RangeSelect value="all" onChange={onChange} />);
    const select = screen.getByRole("combobox") as HTMLSelectElement;
    expect(select.value).toBe("all");
    expect(screen.getAllByRole("option")).toHaveLength(TIME_RANGES.length);
    fireEvent.change(select, { target: { value: "1h" } });
    expect(onChange).toHaveBeenCalledWith("1h");
  });
});
