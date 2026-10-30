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
import { afterEach, describe, it, expect, vi } from "vitest";
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
afterEach(() => {
  cleanup();
  request.mockReset();
  expired.mockReset();
  vi.useRealTimers();
  vi.restoreAllMocks();
});
describe("live rule and Webhook management", () => {
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
    fireEvent.click(screen.getByRole("button", { name: "出站 0" }));
    fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
    fireEvent.change(screen.getByLabelText("ID"), {
      target: { value: "hook" },
    });
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Alarm" },
    });
    fireEvent.change(screen.getByLabelText("Webhook URL"), {
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
    expect(persist).not.toHaveBeenCalled();
    expect(screen.queryByText("Bearer private-value")).toBeNull();
  });
  it("preserves redacted secrets and rejects deleting referenced outbound", async () => {
    server(populated);
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "出站 1" }));
    fireEvent.click(screen.getByRole("button", { name: "编辑 send" }));
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
    fireEvent.click(screen.getByRole("button", { name: "删除 send" }));
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
    fireEvent.click(screen.getByRole("button", { name: "出站 0" }));
    fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
    fireEvent.change(screen.getByLabelText("ID"), {
      target: { value: "send" },
    });
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Send" },
    });
    fireEvent.change(screen.getByLabelText("Webhook URL"), {
      target: { value: "https://example.com/hook" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "规则 0" }));
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    fireEvent.change(screen.getByLabelText("ID"), { target: { value: "hot" } });
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "High" },
    });
    fireEvent.click(screen.getByLabelText(/Send send/));
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
    expect(cfg.rules[0].outbound_ids).toEqual(["send"]);
    expect(cfg.rules[0].enabled).toBe(false);
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
  it("surfaces validation and server failure inside editor without discarding draft", async () => {
    server();
    render(<RulesPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
    fireEvent.change(screen.getByLabelText("ID"), {
      target: { value: "bad id" },
    });
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Keep draft" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    expect(screen.getByRole("alert").textContent).toContain("ID");
    fireEvent.change(screen.getByLabelText("ID"), {
      target: { value: "valid" },
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
    fireEvent.click(screen.getByRole("button", { name: "编辑 hot" }));
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
    fireEvent.click(screen.getByRole("button", { name: "编辑 hot" }));
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
    fireEvent.click(screen.getByRole("button", { name: "删除 hot" }));
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
