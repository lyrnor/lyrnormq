import React from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, it, expect, vi } from "vitest";
import { Drawer } from "../src/components";
afterEach(cleanup);
describe("native details dialog under strict CSP", () => {
  it("opens a genuine native dialog, focuses close and restores prior focus", () => {
    const trigger = document.createElement("button");
    document.body.append(trigger);
    trigger.focus();
    const { unmount } = render(
      <Drawer title="详情" subtitle="client" onClose={vi.fn()}>
        <button>内容按钮</button>
      </Drawer>,
    );
    expect(screen.getByRole("dialog").tagName).toBe("DIALOG");
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "关闭详情" }),
    );
    expect(document.body.classList.contains("overflow-hidden")).toBe(true);
    expect(document.querySelector("style")).toBeNull();
    expect(screen.getByRole("dialog").getAttribute("style")).toBeNull();
    unmount();
    expect(document.activeElement).toBe(trigger);
    expect(document.body.classList.contains("overflow-hidden")).toBe(false);
    trigger.remove();
  });
  it("supports Escape and deterministic focus wrap without CSS injection", () => {
    const close = vi.fn();
    render(
      <Drawer title="详情" subtitle="client" onClose={close}>
        <button>内容按钮</button>
      </Drawer>,
    );
    const first = screen.getByRole("button", { name: "关闭详情" }),
      last = screen.getByRole("button", { name: "内容按钮" });
    first.focus();
    fireEvent.keyDown(first, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(last);
    fireEvent.keyDown(last, { key: "Tab" });
    expect(document.activeElement).toBe(first);
    fireEvent.keyDown(first, { key: "Escape" });
    expect(close).toHaveBeenCalledOnce();
  });
});
