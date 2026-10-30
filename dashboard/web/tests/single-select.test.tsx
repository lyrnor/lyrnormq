import React, { useState } from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SingleSelect } from "../src/components/ui/single-select";
import { Drawer } from "../src/components";
afterEach(cleanup);
const options = [
  { value: "webhook", label: "Webhook" },
  { value: "other", label: "Other target" },
];
describe("styled single selection", () => {
  it("renders a menu in the native dialog top layer and consumes Escape before closing the dialog", async () => {
    const close = vi.fn();
    render(
      <Drawer title="出站" subtitle="" onClose={close}>
        <SingleSelect
          aria-label="出站类型"
          value="webhook"
          onValueChange={vi.fn()}
          options={options}
        />
      </Drawer>,
    );
    const trigger = screen.getByRole("button", { name: "出站类型" });
    fireEvent.keyDown(trigger, { key: "ArrowDown" });
    const item = await screen.findByRole("menuitemradio", { name: "Webhook" });
    expect(item.closest("dialog")).toBe(screen.getByRole("dialog"));
    expect(item.getAttribute("aria-checked")).toBe("true");
    expect(document.querySelector("style")).toBeNull();
    fireEvent.keyDown(item, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
    expect(close).not.toHaveBeenCalled();
    fireEvent.keyDown(trigger, { key: "Escape" });
    expect(close).toHaveBeenCalledOnce();
  });
  it("selects through keyboard and restores the trigger without submitting its form", async () => {
    const submitted = vi.fn((event: React.FormEvent) => event.preventDefault());
    function Example() {
      const [value, setValue] = useState("webhook");
      return (
        <form onSubmit={submitted}>
          <SingleSelect
            aria-label="类型"
            value={value}
            onValueChange={setValue}
            options={options}
          />
        </form>
      );
    }
    render(<Example />);
    const trigger = screen.getByRole("button", { name: "类型" });
    fireEvent.keyDown(trigger, { key: "ArrowDown" });
    const first = await screen.findByRole("menuitemradio", { name: "Webhook" });
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowDown" });
    const second = screen.getByRole("menuitemradio", { name: "Other target" });
    await waitFor(() => expect(document.activeElement).toBe(second));
    fireEvent.keyDown(second, { key: "Enter" });
    await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
    expect(trigger.textContent).toContain("Other target");
    expect(submitted).not.toHaveBeenCalled();
    await waitFor(() => expect(document.activeElement).toBe(trigger));
  });
  it("keeps a disabled selector closed", () => {
    render(
      <SingleSelect
        aria-label="类型"
        value="webhook"
        onValueChange={vi.fn()}
        options={options}
        disabled
      />,
    );
    expect(
      (screen.getByRole("button", { name: "类型" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(screen.queryByRole("menu")).toBeNull();
  });
});
