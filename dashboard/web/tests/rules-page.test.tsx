import React from "react";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { RulesPage } from "../src/RulesPage";
import { api, ApiError } from "../src/api";
import { defaultOutbound, type RulesConfig } from "../src/rules-model";
vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));
const request = vi.mocked(api);
const empty: RulesConfig = { schema_version: 2, outbounds: [], rules: [] };
const populated: RulesConfig = {
  schema_version: 2,
  outbounds: [
    {
      ...defaultOutbound(),
      id: "send",
      name: "Send alarm",
      config: {
        url: "https://example.com/alarm",
        headers: { Authorization: "[redacted]" },
      },
    },
  ],
  rules: [
    {
      id: "hot",
      name: "High temperature",
      sql: 'SELECT payload.temperature FROM "devices/#" WHERE payload.temperature > 80',
      enabled: true,
      outbound_ids: ["send"],
    },
  ],
};
const status = {
  running: true,
  mode: "best_effort",
  queued_events: 2,
  queued_bytes: 128,
  received: 12,
  matched: 8,
  dropped: 1,
  errors: 0,
  outbounds: [
    {
      id: "send",
      type: "webhook",
      queued: 2,
      delivered: 6,
      failed: 0,
      retried: 1,
      dropped: 0,
      last_error: null,
    },
  ],
};
function server(initial: RulesConfig = empty) {
  let stored = structuredClone(initial);
  request.mockImplementation(async (path, _token, _signal, options) => {
    if (options?.method === "PUT") {
      stored = structuredClone(options.body as RulesConfig);
      return stored as never;
    }
    if (path.endsWith("config")) return stored as never;
    if (path.endsWith("status")) return status as never;
    if (path.endsWith("test"))
      return { matched: true, output: { temperature: 85 } } as never;
    throw new Error("unexpected endpoint");
  });
}
const expired = vi.fn();
async function ready() {
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "新建规则" })).toBeTruthy(),
  );
}
beforeEach(() => {
  // Radix form controls measure their hidden input; jsdom has no layout observer.
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  request.mockReset();
  expired.mockReset();
  vi.useRealTimers();
  vi.restoreAllMocks();
});
describe("live rule and Webhook management", () => {
  it("creates without exposing an ID field", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    expect(screen.queryByLabelText("ID")).toBeNull();
  });

  it("shows a plain disabled state without implementation details", async () => {
    request.mockRejectedValue(
      new ApiError(503, "规则引擎未启用", "rules_disabled"),
    );
    render(<RulesPage token="token" expired={expired} />);
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain("规则引擎未启用"),
    );
    expect(screen.queryByRole("button", { name: "新建规则" })).toBeNull();
    expect(screen.queryByText(/outbox|Best effort|有界异步旁路/)).toBeNull();
    expect(screen.queryByText(/delivery_id|--rules-config/)).toBeNull();
  });
  it("creates outbound with password inputs, never writes credentials into storage", async () => {
    server();
    const persist = vi.spyOn(Storage.prototype, "setItem");
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.mouseDown(screen.getByRole("tab", { name: "出站 0" }), {
      button: 0,
      ctrlKey: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Alarm" },
    });
    fireEvent.change(screen.getByLabelText("Webhook 地址"), {
      target: { value: "https://example.com/hook" },
    });
    fireEvent.click(screen.getByRole("button", { name: "添加请求头" }));
    fireEvent.change(screen.getByLabelText("请求头名称 1"), {
      target: { value: "Authorization" },
    });
    const value = screen.getByLabelText("请求头值 1") as HTMLInputElement;
    expect(value.type).toBe("password");
    fireEvent.change(value, { target: { value: "Bearer private-value" } });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const call = request.mock.calls.find(
      ([, , , options]) => options?.method === "PUT",
    );
    expect(
      (call?.[3]?.body as RulesConfig).outbounds[0].config.headers
        .Authorization,
    ).toBe("Bearer private-value");
    const savedOutbound = (call?.[3]?.body as RulesConfig).outbounds[0];
    expect(savedOutbound.id).toMatch(/^out_[A-Za-z0-9-]+$/);
    expect(screen.queryByText(savedOutbound.id)).toBeNull();
    expect(persist).not.toHaveBeenCalled();
    expect(screen.queryByText("Bearer private-value")).toBeNull();
  });
  it("preserves redacted secrets and rejects deleting referenced outbound", async () => {
    server(populated);
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.mouseDown(screen.getByRole("tab", { name: "出站 1" }), {
      button: 0,
      ctrlKey: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "编辑 Send alarm" }));
    expect(
      (screen.getByLabelText("请求头值 1") as HTMLInputElement).value,
    ).toBe("[redacted]");
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Renamed" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(
      (
        request.mock.calls.find(
          ([, , , options]) => options?.method === "PUT",
        )?.[3]?.body as RulesConfig
      ).outbounds[0].config.headers.Authorization,
    ).toBe("[redacted]");
    expect(
      (
        request.mock.calls.find(
          ([, , , options]) => options?.method === "PUT",
        )?.[3]?.body as RulesConfig
      ).outbounds[0].id,
    ).toBe("send");
    expect(screen.queryByText("send")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "删除 Renamed" }));
    expect(screen.getByRole("alert").textContent).toContain("引用");
    expect(
      (screen.getByRole("button", { name: "确认删除" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });
  it("creates outbound and rule with exact references then disables rule", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.mouseDown(screen.getByRole("tab", { name: "出站 0" }), {
      button: 0,
      ctrlKey: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Send" },
    });
    fireEvent.change(screen.getByLabelText("Webhook 地址"), {
      target: { value: "https://example.com/hook" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    fireEvent.mouseDown(screen.getByRole("tab", { name: "规则 0" }), {
      button: 0,
      ctrlKey: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "High" },
    });
    fireEvent.click(screen.getByRole("checkbox", { name: "Send" }));
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "停用" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "启用" })).toBeTruthy(),
    );
    const writes = request.mock.calls.filter(
      ([, , , options]) => options?.method === "PUT",
    );
    expect(writes).toHaveLength(3);
    const cfg = writes[2][3]?.body as RulesConfig;
    expect(cfg.rules[0].outbound_ids).toEqual([cfg.outbounds[0].id]);
    expect(cfg.outbounds[0].id).toMatch(/^out_[A-Za-z0-9-]+$/);
    expect(cfg.rules[0].id).toMatch(/^rule_[A-Za-z0-9-]+$/);
    expect(cfg.rules[0].id).not.toBe(cfg.outbounds[0].id);
    expect(cfg.rules[0].enabled).toBe(false);
  });
  it("binds only the selected identifier when outbound names are identical", async () => {
    server({
      ...populated,
      outbounds: [
        {
          ...populated.outbounds[0],
          name: "Shared destination",
          config: { url: "https://example.com/first", headers: {} },
        },
        {
          ...populated.outbounds[0],
          id: "second-outbound",
          name: "Shared destination",
          config: { url: "https://example.com/second", headers: {} },
        },
      ],
    });
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Only second" },
    });
    const first = screen.getByRole("checkbox", {
      name: /https:\/\/example.com\/first/,
    });
    const second = screen.getByRole("checkbox", {
      name: /https:\/\/example.com\/second/,
    });
    expect(first.getAttribute("aria-checked")).toBe("false");
    expect(second.getAttribute("aria-checked")).toBe("false");
    fireEvent.click(second);
    expect(first.getAttribute("aria-checked")).toBe("false");
    expect(second.getAttribute("aria-checked")).toBe("true");
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const saved = request.mock.calls.find(
      (call) => call[3]?.method === "PUT",
    )?.[3]?.body as RulesConfig;
    expect(
      saved.rules.find((rule) => rule.name === "Only second")?.outbound_ids,
    ).toEqual(["second-outbound"]);
    expect(saved.rules.find((rule) => rule.id === "hot")?.outbound_ids).toEqual(
      ["send"],
    );
  });
  it("rejects unknown outbound types without interpreting them as Webhook", async () => {
    server({
      ...empty,
      outbounds: [{ ...defaultOutbound(), type: "kafka" }],
    } as unknown as RulesConfig);
    render(<RulesPage token="token" expired={expired} />);
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain("暂不受支持"),
    );
    expect(screen.queryByRole("button", { name: "新建规则" })).toBeNull();
    expect(request.mock.calls.every((call) => !call[3])).toBe(true);
  });
  it("SQL dry run only calls test endpoint and escapes output", async () => {
    server();
    request.mockImplementation(async (path, _token, _signal, options) => {
      if (path.endsWith("config")) return empty as never;
      if (path.endsWith("status")) return status as never;
      if (options?.method === "POST")
        return {
          matched: true,
          output: { value: "<img src=x onerror=alert(1)>" },
        } as never;
      throw new Error("unexpected");
    });
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "测试 SQL" }));
    fireEvent.click(screen.getByRole("button", { name: "运行测试" }));
    await waitFor(() => expect(screen.getByText("已匹配")).toBeTruthy());
    expect(screen.getByText(/<img src=x onerror=alert/)).toBeTruthy();
    expect(document.querySelector("img")).toBeNull();
    const writes = request.mock.calls.filter(([, , , options]) => options);
    expect(writes).toHaveLength(1);
    expect(writes[0][0]).toBe("/api/v1/rules/test");
    expect(
      (writes[0][3]?.body as { event: { payload: number[] } }).event.payload,
    ).toEqual(Array.from(new TextEncoder().encode('{"temperature":85}')));
  });
  it("keeps the generated ID across validation failure and a failed HTTP save", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    expect(screen.queryByLabelText("ID")).toBeNull();
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Keep draft" },
    });
    fireEvent.change(screen.getByLabelText("SQL"), { target: { value: " " } });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    expect(screen.getByRole("alert").textContent).toContain("SQL");
    expect(
      request.mock.calls.filter((call) => call[3]?.method === "PUT"),
    ).toHaveLength(0);
    fireEvent.change(screen.getByLabelText("SQL"), {
      target: { value: 'SELECT * FROM "test/#"' },
    });
    request.mockRejectedValueOnce(new ApiError(400, "invalid"));
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() =>
      expect(
        within(screen.getByRole("dialog")).getByRole("alert").textContent,
      ).toContain("保存未确认"),
    );
    expect((screen.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "Keep draft",
    );
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const writes = request.mock.calls.filter(
      (call) => call[3]?.method === "PUT",
    );
    expect(writes).toHaveLength(2);
    expect((writes[0][3]?.body as RulesConfig).rules[0].id).toBe(
      (writes[1][3]?.body as RulesConfig).rules[0].id,
    );
  });
  it("preserves existing rule identifiers and bindings when renaming", async () => {
    server(populated);
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(
      screen.getByRole("button", { name: "编辑 High temperature" }),
    );
    expect(screen.queryByLabelText("ID")).toBeNull();
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Renamed rule" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const saved = request.mock.calls.find(
      (call) => call[3]?.method === "PUT",
    )?.[3]?.body as RulesConfig;
    expect(saved.rules[0].id).toBe("hot");
    expect(saved.rules[0].outbound_ids).toEqual(["send"]);
    expect(screen.queryByText("hot")).toBeNull();
  });
  it("shows a controlled creation failure if secure identifiers cannot be generated", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    vi.stubGlobal("crypto", undefined);
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    expect(screen.getByRole("alert").textContent).toContain("暂时无法创建");
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(
      request.mock.calls.filter((call) => call[3]?.method === "PUT"),
    ).toHaveLength(0);
  });
  it("switches real tab panels through keyboard without writing configuration", async () => {
    server(populated);
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    const first = screen.getByRole("tab", { name: "规则 1" });
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowRight" });
    const next = screen.getByRole("tab", { name: "出站 1" });
    await waitFor(() =>
      expect(next.getAttribute("aria-selected")).toBe("true"),
    );
    expect(screen.getByRole("tabpanel").getAttribute("aria-labelledby")).toBe(
      next.id,
    );
    expect(
      request.mock.calls.filter((call) => call[3]?.method === "PUT"),
    ).toHaveLength(0);
  });
  it("clears auth on401 and aborts requests when leaving the page", async () => {
    const signals: AbortSignal[] = [];
    request.mockImplementation((_p, _t, signal) => {
      signals.push(signal!);
      return new Promise(() => {});
    });
    const view = render(<RulesPage token="token" expired={expired} />);
    expect(signals).toHaveLength(2);
    view.unmount();
    expect(signals.every((s) => s.aborted)).toBe(true);
    request.mockRejectedValue(new ApiError(401, "expired"));
    render(<RulesPage token="token" expired={expired} />);
    await waitFor(() => expect(expired).toHaveBeenCalled());
  });
  it("polls status without overwriting an open form and stops while hidden", async () => {
    server(populated);
    vi.useFakeTimers();
    render(<RulesPage token="token" expired={expired} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    fireEvent.click(
      screen.getByRole("button", { name: "编辑 High temperature" }),
    );
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Unsaved draft" },
    });
    const before = request.mock.calls.length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(request.mock.calls.length).toBe(before + 1);
    expect((screen.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "Unsaved draft",
    );
    vi.spyOn(document, "hidden", "get").mockReturnValue(true);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });
    expect(request.mock.calls.length).toBe(before + 1);
  });
  it("marks failed statistics stale and clears that warning after a successful sample", async () => {
    server(populated);
    vi.useFakeTimers();
    render(<RulesPage token="token" expired={expired} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    request.mockRejectedValueOnce(new ApiError(503, "unavailable"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(screen.getByRole("alert").textContent).toContain("不能视为实时状态");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(screen.queryByRole("alert")).toBeNull();
  });
  it("shows stopped execution explicitly while keeping published configuration inspectable", async () => {
    request.mockImplementation(
      async (path) =>
        (path.endsWith("config")
          ? populated
          : { ...status, running: false }) as never,
    );
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    expect(screen.getByRole("alert").textContent).toContain("规则服务已停止");
    expect(screen.getByText("High temperature")).toBeTruthy();
    expect(
      (screen.getByRole("button", { name: "新建规则" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "停用" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "测试 SQL" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    fireEvent.click(
      screen.getByRole("button", { name: "编辑 High temperature" }),
    );
    expect(
      (screen.getByRole("button", { name: "保存并应用" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(request.mock.calls.every((call) => !call[3])).toBe(true);
  });
  it("disables an already-open delete confirmation if execution stops", async () => {
    server(populated);
    vi.useFakeTimers();
    render(<RulesPage token="token" expired={expired} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    fireEvent.click(
      screen.getByRole("button", { name: "删除 High temperature" }),
    );
    expect(
      (screen.getByRole("button", { name: "确认删除" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    request.mockResolvedValueOnce({ ...status, running: false } as never);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(
      (screen.getByRole("button", { name: "确认删除" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });
});

describe("Webhook basic and advanced settings", () => {
  const writes = () =>
    request.mock.calls.filter((call) => call[3]?.method === "PUT");
  const saved = () => (writes().at(-1)?.[3]?.body as RulesConfig).outbounds[0];
  async function fresh() {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.mouseDown(screen.getByRole("tab", { name: "出站 0" }), {
      button: 0,
      ctrlKey: false,
    });
    fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Webhook" },
    });
    fireEvent.change(screen.getByLabelText("Webhook 地址"), {
      target: { value: "https://example.com/hook" },
    });
  }
  it("only shows business fields initially and saves the system defaults", async () => {
    await fresh();
    expect(screen.getByRole("button", { name: "添加请求头" })).toBeTruthy();
    for (const label of [
      "批量条数",
      "批量等待（ms）",
      "请求超时（ms）",
      "最多尝试次数",
      "重试基础间隔（ms）",
      "队列容量（条）",
      "出站类型",
      "启用",
    ])
      expect(screen.queryByLabelText(label)).toBeNull();
    const trigger = screen.getByRole("button", { name: "高级设置" });
    expect(trigger.getAttribute("type")).toBe("button");
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(saved().delivery).toEqual({
      batch_size: 1,
      batch_wait_ms: 0,
      queue_capacity: 256,
      timeout_ms: 3000,
      max_attempts: 3,
      retry_base_ms: 200,
    });
  });
  it("retains advanced edits through collapse with no editable queue capacity", async () => {
    await fresh();
    const trigger = screen.getByRole("button", { name: "高级设置" });
    fireEvent.click(trigger);
    expect(trigger.getAttribute("aria-expanded")).toBe("true");
    for (const [label, value] of Object.entries({
      批量条数: 12,
      "批量等待（ms）": 25,
      "请求超时（ms）": 500,
      最多尝试次数: 2,
      "重试基础间隔（ms）": 30,
    }))
      fireEvent.change(screen.getByLabelText(label), { target: { value } });
    expect(screen.getByRole("switch", { name: "启用" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "出站类型" })).toBeTruthy();
    expect(screen.queryByLabelText("队列容量（条）")).toBeNull();
    fireEvent.click(trigger);
    expect(screen.queryByLabelText("批量条数")).toBeNull();
    fireEvent.click(trigger);
    expect((screen.getByLabelText("批量条数") as HTMLInputElement).value).toBe(
      "12",
    );
    fireEvent.click(trigger);
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(saved().delivery).toEqual({
      batch_size: 12,
      batch_wait_ms: 25,
      timeout_ms: 500,
      max_attempts: 2,
      retry_base_ms: 30,
      queue_capacity: 256,
    });
  });
  it.each([
    [100, 50],
    [7, 15],
  ])(
    "preserves existing batch %i/wait %i and queue capacity without opening advanced",
    async (batchSize, batchWaitMs) => {
      const initial = structuredClone(populated);
      initial.outbounds[0].enabled = false;
      initial.outbounds[0].delivery = {
        batch_size: batchSize,
        batch_wait_ms: batchWaitMs,
        timeout_ms: 1200,
        max_attempts: 4,
        retry_base_ms: 45,
        queue_capacity: 777,
      };
      server(initial);
      render(<RulesPage token="token" expired={expired} />);
      await ready();
      fireEvent.mouseDown(screen.getByRole("tab", { name: "出站 1" }), {
        button: 0,
        ctrlKey: false,
      });
      expect(screen.queryByText(/每批.*等待/)).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "编辑 Send alarm" }));
      expect(
        screen
          .getByRole("button", { name: "高级设置" })
          .getAttribute("aria-expanded"),
      ).toBe("false");
      fireEvent.change(screen.getByLabelText("名称"), {
        target: { value: "Renamed only" },
      });
      fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
      await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
      expect(saved().delivery).toEqual(initial.outbounds[0].delivery);
      expect(saved().enabled).toBe(false);
      expect(saved().id).toBe("send");
      expect(saved().config.headers.Authorization).toBe("[redacted]");
    },
  );
  it("expands hidden invalid tuning instead of submitting it", async () => {
    await fresh();
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    fireEvent.change(screen.getByLabelText("批量条数"), {
      target: { value: "0" },
    });
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() =>
      expect(
        screen
          .getByRole("button", { name: "高级设置" })
          .getAttribute("aria-expanded"),
      ).toBe("true"),
    );
    expect(screen.getByRole("alert").textContent).toContain("批量条数");
    expect(document.activeElement).toBe(screen.getByLabelText("批量条数"));
    expect(writes()).toHaveLength(0);
  });
  it("keeps the rule enable switch and SQL immediately available", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    expect(screen.getByRole("switch", { name: "启用" })).toBeTruthy();
    expect(screen.getByLabelText("SQL")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "高级设置" })).toBeNull();
  });
});
