import React from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "../src/App";
import { api } from "../src/api";
import type { Overview, Page, Session, Subscription } from "../src/types";

vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));

const request = vi.mocked(api);
const token = "a".repeat(32);

const overview: Overview = {
  ready: true,
  uptime_seconds: 12,
  online_sessions: 2,
  offline_sessions: 1,
  subscriptions: 3,
  retained_messages: 1,
  messages_in_total: 10,
  messages_out_total: 4,
  bytes_in_total: 100,
  bytes_out_total: 40,
  state_bytes: 2048,
  sampled_at_millis: 1_700_000_000_000,
  instance: {
    version: "0.1.0",
    listen: "127.0.0.1:18831",
    transport: "tcp",
    protocol: "both",
    storage: "memory",
    tls: false,
    mtls: false,
    auth_policy: false,
    enhanced_auth: false,
  },
  resources: { cpu_percent: null, rss_bytes: null },
  capabilities: {
    events: false,
    resource_metrics: false,
    mutations: false,
    cluster: true,
  },
  traffic_scope: "local",
  topology: {
    revision: 3,
    leader: 1,
    term: 1,
    last_applied: 10,
    data_storage: "memory",
    data_generation: 1,
    clients: 3,
    routes: 3,
    self_node_id: 101,
    excluded: [],
    nodes: [
      {
        id: 101,
        roles: ["broker"],
        broker_id: 101,
        address: "http://127.0.0.1:19101",
        applied: 1,
        expires_at: 9_999_999_999_999,
        max_lease_ms: 3000,
        lease_remaining_ms: 2000,
        clients: 2,
        excluded: false,
        self: true,
        leader: false,
        online: true,
      },
      {
        id: 102,
        roles: ["broker"],
        broker_id: 102,
        address: "http://127.0.0.2:19101",
        applied: 1,
        expires_at: 9_999_999_999_999,
        max_lease_ms: 3000,
        lease_remaining_ms: 2000,
        clients: 1,
        excluded: false,
        self: false,
        leader: false,
        online: true,
      },
    ],
  },
};

const sessions: Page<Session> = {
  total: 2,
  offset: 0,
  limit: 25,
  items: [
    {
      client_id: "sensor-a",
      online: true,
      protocol: "5.0",
      principal: "anon",
      persistent: false,
      subscriptions: 1,
      queued: 0,
      inflight: 0,
      receive_maximum: 16,
      session_expiry_interval: 0,
      node_id: 101,
      directory: false,
    },
    {
      client_id: "sensor-b",
      online: false,
      protocol: "unknown",
      principal: "",
      persistent: true,
      subscriptions: 1,
      queued: 0,
      inflight: 0,
      receive_maximum: null,
      session_expiry_interval: 0,
      node_id: 102,
      directory: true,
    },
  ],
};

const subscriptions: Page<Subscription> = {
  total: 2,
  offset: 0,
  limit: 25,
  items: [
    {
      client_id: "sensor-a",
      filter: "sensors/#",
      qos: 0,
      no_local: false,
      retain_as_published: false,
      retain_handling: 0,
      identifier: null,
      node_id: 101,
      directory: true,
    },
    {
      client_id: "sensor-b",
      filter: "metrics/+",
      qos: 0,
      no_local: false,
      retain_as_published: false,
      retain_handling: 0,
      identifier: null,
      node_id: 102,
      directory: true,
    },
  ],
};

function mockApi() {
  request.mockImplementation(async (path) => {
    if (path.includes("overview")) return overview as never;
    if (path.includes("configuration"))
      return { instance: overview.instance, limits: {} } as never;
    if (path.includes("sessions")) return sessions as never;
    if (path.includes("subscriptions")) return subscriptions as never;
    if (path.includes("retained"))
      return { items: [], total: 0, offset: 0, limit: 25 } as never;
    throw new Error(path);
  });
}

describe("cluster directory dashboard pages", () => {
  afterEach(() => {
    cleanup();
    window.location.hash = "";
    vi.clearAllMocks();
  });

  async function login() {
    mockApi();
    render(<App />);
    fireEvent.change(screen.getByLabelText("管理令牌"), {
      target: { value: token },
    });
    fireEvent.click(screen.getByRole("button", { name: /进入控制台/ }));
    await screen.findByText("集群在线会话");
  }

  it("labels cluster overview totals separately from local traffic", async () => {
    await login();
    expect(screen.getByText("本节点入站")).toBeTruthy();
    expect(screen.getByText("本节点出站")).toBeTruthy();
    expect(screen.getByText("集群会话合计")).toBeTruthy();
  });

  it("renders node ownership on connections and topics", async () => {
    await login();
    window.location.hash = "#connections";
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    await screen.findByText("sensor-a");
    expect(screen.getByText("#101")).toBeTruthy();
    expect(screen.getByText("#102")).toBeTruthy();
    expect(screen.getByLabelText("筛选所属节点")).toBeTruthy();
    window.location.hash = "#topics";
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    await screen.findByText("sensors/#");
    await waitFor(() =>
      expect(screen.getByText("集群目录 · 每 5 秒更新")).toBeTruthy(),
    );
    expect(screen.getAllByText("#101").length).toBeGreaterThan(0);
  });
});
