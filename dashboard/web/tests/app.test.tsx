import React from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, it, expect, vi } from "vitest";
import { App } from "../src/App";
import { api, ApiError } from "../src/api";
vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));
const request = vi.mocked(api);
const instance = {
  version: "0.1.0",
  listen: "127.0.0.1:1883",
  transport: "TCP",
  protocol: "both",
  storage: "memory",
  tls: false,
  mtls: false,
  auth_policy: false,
  enhanced_auth: false,
};
const overview = {
  ready: true,
  uptime_seconds: 100,
  online_sessions: 1,
  offline_sessions: 0,
  subscriptions: 2,
  retained_messages: 0,
  messages_in_total: 123,
  messages_out_total: 234,
  bytes_in_total: 1000,
  bytes_out_total: 2000,
  state_bytes: 512,
  sampled_at_millis: 10000,
  instance,
  resources: { cpu_percent: null, rss_bytes: null },
  capabilities: { events: false, resource_metrics: false, mutations: false },
};
const limits = {
  max_connections: 1024,
  max_sessions: 1024,
  max_packet_size: 1048576,
  max_subscriptions: 256,
  max_retained: 10000,
  max_inflight: 32,
  outbound_capacity: 64,
  max_handshakes: 128,
  max_auth_tasks: 4,
  auth_rate_per_second: 1000,
  auth_burst: 1000,
  handshake_timeout_ms: 10000,
  write_timeout_ms: 5000,
  topic_alias_maximum: 32,
};
const token = "a".repeat(32);
function responses() {
  request.mockImplementation(
    async (path) =>
      (path.includes("overview")
        ? overview
        : path.includes("configuration")
          ? { instance, limits }
          : { items: [], total: 0, offset: 0, limit: 25 }) as never,
  );
}
async function login() {
  fireEvent.change(screen.getByLabelText("管理令牌"), {
    target: { value: token },
  });
  fireEvent.click(screen.getByRole("button", { name: /进入控制台/ }));
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "退出登录" })).toBeTruthy(),
  );
}
afterEach(() => {
  cleanup();
  request.mockReset();
  vi.restoreAllMocks();
  location.hash = "";
});
describe("memory-only management login", () => {
  it("renders real data and clears authenticated views on logout without persisting the token", async () => {
    responses();
    const storage = vi.spyOn(Storage.prototype, "setItem");
    render(<App />);
    await login();
    await waitFor(() =>
      expect(screen.getAllByText("127.0.0.1:1883").length).toBeGreaterThan(0),
    );
    expect(storage).not.toHaveBeenCalled();
    expect(screen.queryByText(token)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "退出登录" }));
    expect(screen.getByLabelText("管理令牌")).toBeTruthy();
    expect(screen.queryByText("127.0.0.1:1883")).toBeNull();
    expect((screen.getByLabelText("管理令牌") as HTMLInputElement).value).toBe(
      "",
    );
  });
  it("reports invalid credentials without entering the console", async () => {
    request.mockRejectedValue(new ApiError(401, "管理令牌无效"));
    render(<App />);
    fireEvent.change(screen.getByLabelText("管理令牌"), {
      target: { value: token },
    });
    fireEvent.click(screen.getByRole("button", { name: /进入控制台/ }));
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain("管理令牌无效"),
    );
    expect(screen.queryByRole("button", { name: "退出登录" })).toBeNull();
  });
  it("returns to login if a console request receives 401", async () => {
    request
      .mockResolvedValueOnce(overview)
      .mockRejectedValue(new ApiError(401, "expired"));
    render(<App />);
    fireEvent.change(screen.getByLabelText("管理令牌"), {
      target: { value: token },
    });
    fireEvent.click(screen.getByRole("button", { name: /进入控制台/ }));
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain("已失效"),
    );
    expect(screen.queryByRole("button", { name: "退出登录" })).toBeNull();
  });
  it("escapes broker strings and labels restored negotiation unknown", async () => {
    location.hash = "#connections";
    const client = "<img src=x onerror=alert(1)>";
    const session = {
      client_id: client,
      online: false,
      protocol: "unknown",
      principal: "user:<script>",
      persistent: true,
      subscriptions: 1,
      queued: 0,
      inflight: 0,
      receive_maximum: null,
      session_expiry_interval: 3600,
    };
    request.mockImplementation(
      async (path) =>
        (path.includes("overview")
          ? overview
          : path.includes("configuration")
            ? { instance, limits }
            : path.includes("/sessions?")
              ? { items: [session], total: 1, offset: 0, limit: 25 }
              : session) as never,
    );
    const { container } = render(<App />);
    await login();
    await waitFor(() => expect(screen.getByText(client)).toBeTruthy());
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("script")).toBeNull();
    expect(screen.getByText("未协商")).toBeTruthy();
    fireEvent.click(
      screen.getByRole("button", { name: `查看 ${client} 详情` }),
    );
    await waitFor(() =>
      expect(screen.getByRole("dialog").textContent).toContain("未协商"),
    );
    expect(container.querySelectorAll("[style], style")).toHaveLength(0);
    fireEvent.keyDown(screen.getByRole("button", { name: "关闭详情" }), {
      key: "Escape",
    });
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
