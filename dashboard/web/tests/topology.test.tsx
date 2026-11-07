import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TopologyPanel } from "../src/TopologyPanel";
import type { Topology } from "../src/types";

const cluster: Topology = {
  revision: 12,
  leader: 2,
  term: 4,
  last_applied: 90,
  data_storage: "rocksdb",
  data_generation: 3,
  clients: 8,
  routes: 5,
  self_node_id: 101,
  excluded: [],
  nodes: [
    {
      id: 1,
      roles: ["controller"],
      controller_id: 1,
      address: "http://127.0.0.1:19001",
      control_address: "http://127.0.0.1:19001",
      applied: 0,
      expires_at: 0,
      max_lease_ms: 0,
      lease_remaining_ms: 0,
      clients: 0,
      excluded: false,
      self: false,
      leader: false,
      online: true,
    },
    {
      id: 2,
      roles: ["controller"],
      controller_id: 2,
      address: "http://127.0.0.2:19001",
      control_address: "http://127.0.0.2:19001",
      applied: 0,
      expires_at: 0,
      max_lease_ms: 0,
      lease_remaining_ms: 0,
      clients: 0,
      excluded: false,
      self: false,
      leader: true,
      online: true,
    },
    {
      id: 3,
      roles: ["controller"],
      controller_id: 3,
      address: "http://127.0.0.3:19001",
      control_address: "http://127.0.0.3:19001",
      applied: 0,
      expires_at: 0,
      max_lease_ms: 0,
      lease_remaining_ms: 0,
      clients: 0,
      excluded: false,
      self: false,
      leader: false,
      online: true,
    },
    {
      id: 101,
      roles: ["broker"],
      broker_id: 101,
      address: "http://127.0.0.1:19101",
      broker_address: "http://127.0.0.1:19101",
      applied: 12,
      expires_at: 9_999_999_999_999,
      max_lease_ms: 3000,
      lease_remaining_ms: 2400,
      clients: 3,
      disk_id: "abcdef0123456789",
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
      broker_address: "http://127.0.0.2:19101",
      applied: 12,
      expires_at: 9_999_999_999_999,
      max_lease_ms: 3000,
      lease_remaining_ms: 2200,
      clients: 5,
      excluded: false,
      self: false,
      leader: false,
      online: true,
    },
    {
      id: 103,
      roles: ["broker"],
      broker_id: 103,
      address: "http://127.0.0.3:19101",
      broker_address: "http://127.0.0.3:19101",
      applied: 11,
      expires_at: 9_999_999_999_999,
      max_lease_ms: 3000,
      lease_remaining_ms: 1800,
      clients: 0,
      excluded: false,
      self: false,
      leader: false,
      online: true,
    },
  ],
};

describe("TopologyPanel", () => {
  it("renders split controller and broker planes", () => {
    render(<TopologyPanel topology={cluster} />);
    expect(screen.getByText(/集群拓扑 · 6 成员/)).toBeTruthy();
    expect(screen.getByText("Leader #2")).toBeTruthy();
    expect(screen.getByText(/控制面 · Raft 仲裁/)).toBeTruthy();
    expect(screen.getByText(/数据面 · 副本组/)).toBeTruthy();
    expect(screen.getByText(/元数据 \/ 租约 → Leader/)).toBeTruthy();
    expect(screen.getByText(/数据副本/)).toBeTruthy();
    expect(screen.getAllByText("Controller").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Broker").length).toBeGreaterThan(0);
    expect(screen.getAllByText("成员 #101").length).toBeGreaterThan(0);
    expect(screen.getAllByText("本机").length).toBeGreaterThan(0);
    expect(screen.getByText(/broker 127.0.0.1:19101/)).toBeTruthy();
    expect(screen.getByText(/control 127.0.0.2:19001/)).toBeTruthy();
  });
});
