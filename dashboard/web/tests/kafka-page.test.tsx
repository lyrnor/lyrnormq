import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { RulesPage } from "../src/RulesPage";
import { api } from "../src/api";
vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));
const request = vi.mocked(api);
const kafka = {
  id: "kafka-out",
  name: "Kafka stream",
  enabled: true,
  type: "kafka",
  config: {
    brokers: ["localhost:9092"],
    topic: "events",
    key_field: null,
    security: {
      protocol: "plaintext",
      sasl_mechanism: null,
      username: null,
      password: null,
      ca_pem: null,
      certificate_pem: null,
      private_key_pem: null,
      private_key_password: null,
    },
    compression: "lz4",
    linger_ms: 1,
    batch_bytes: 65536,
  },
  delivery: {
    concurrency: 128,
    batch_size: 1,
    batch_wait_ms: 0,
    queue_capacity: 256,
    timeout_ms: 30000,
    max_attempts: 3,
    retry_base_ms: 200,
  },
};
const status = {
  running: true,
  mode: "best_effort",
  inflight_requests: 0,
  queued_events: 0,
  queued_bytes: 0,
  received: 0,
  matched: 0,
  dropped: 0,
  errors: 0,
  outbounds: [],
};
beforeEach(() => {
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
  vi.clearAllMocks();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
function server(
  config: unknown = { schema_version: 2, rules: [], outbounds: [kafka] },
) {
  request.mockImplementation(async (path, _token, _signal, options) =>
    options?.method === "PUT"
      ? (options.body as never)
      : path.endsWith("config")
        ? (config as never)
        : (status as never),
  );
}
async function outboundTab() {
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "新建规则" })).toBeTruthy(),
  );
  fireEvent.mouseDown(screen.getByRole("tab", { name: /出站 / }), {
    button: 0,
    ctrlKey: false,
  });
}
describe("Kafka outbound form", () => {
  it("loads Kafka as a supported editable target", async () => {
    server();
    render(<RulesPage token="token" expired={() => {}} />);
    await outboundTab();
    expect(screen.getByText(/Kafka · localhost:9092/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "编辑 Kafka stream" }));
    expect(screen.getByLabelText("Kafka Topic")).toHaveProperty(
      "value",
      "events",
    );
    expect(screen.getByLabelText("Broker 地址")).toHaveProperty(
      "value",
      "localhost:9092",
    );
  });
});

async function select(label: string, option: string) {
  const trigger = screen.getByRole("button", { name: label });
  fireEvent.keyDown(trigger, { key: "ArrowDown" });
  const item = await screen.findByRole("menuitemradio", {
    name: option,
  });
  item.focus();
  fireEvent.keyDown(item, { key: "Enter" });
  await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
}
const saved = () =>
  request.mock.calls.filter((c) => c[3]?.method === "PUT").at(-1)?.[3]
    ?.body as { outbounds: (typeof kafka)[] };
async function freshKafka() {
  server({ schema_version: 2, rules: [], outbounds: [] });
  render(<RulesPage token="token" expired={() => {}} />);
  await outboundTab();
  fireEvent.click(screen.getByRole("button", { name: "新建出站" }));
  fireEvent.change(screen.getByLabelText("名称"), {
    target: { value: "Kafka destination" },
  });
  await select("出站类型", "Kafka");
  fireEvent.change(screen.getByLabelText("Broker 地址"), {
    target: { value: "kafka-1:9092\nkafka-2:9092" },
  });
  fireEvent.change(screen.getByLabelText("Kafka Topic"), {
    target: { value: "events" },
  });
}
describe("Kafka configuration lifecycle", () => {
  it("creates a Kafka outbound with system-managed limits and no exposed internal identifiers", async () => {
    await freshKafka();
    expect(screen.queryByLabelText("ID")).toBeNull();
    expect(screen.queryByLabelText("并发发送数")).toBeNull();
    expect(screen.queryByLabelText("队列容量（条）")).toBeNull();
    expect(
      screen
        .getByRole("button", { name: "高级设置" })
        .getAttribute("aria-expanded"),
    ).toBe("false");
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const o = saved().outbounds[0];
    expect(o.id).toMatch(/^out_/);
    expect(o.type).toBe("kafka");
    expect(o.config).toMatchObject({
      brokers: ["kafka-1:9092", "kafka-2:9092"],
      topic: "events",
      key_field: null,
      compression: "lz4",
      linger_ms: 1,
      batch_bytes: 65536,
    });
    expect(o.delivery).toEqual(kafka.delivery);
  });
  it("clears unrelated protocol fields and restores each outbound type defaults", async () => {
    await freshKafka();
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    fireEvent.change(screen.getByLabelText("并发发送数"), {
      target: { value: 500 },
    });
    await select("出站类型", "Webhook");
    expect(screen.getByLabelText("名称")).toHaveProperty(
      "value",
      "Kafka destination",
    );
    expect(screen.queryByLabelText("Broker 地址")).toBeNull();
    fireEvent.change(screen.getByLabelText("Webhook 地址"), {
      target: { value: "https://example.com/hook" },
    });
    await select("出站类型", "Kafka");
    expect(screen.getByLabelText("Broker 地址")).toHaveProperty("value", "");
    expect(screen.queryByLabelText("Webhook 地址")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    expect(screen.getByLabelText("并发发送数")).toHaveProperty("value", "128");
    expect(screen.getByLabelText("投递超时（ms）")).toHaveProperty(
      "value",
      "30000",
    );
    expect(screen.queryByLabelText("批量条数")).toBeNull();
    expect(screen.queryByLabelText("批量等待（ms）")).toBeNull();
  });
  it("shows SASL/TLS fields conditionally, saves matching security and never persists credentials in browser storage", async () => {
    const storage = vi.spyOn(Storage.prototype, "setItem");
    await freshKafka();
    await select("安全连接", "SASL + TLS");
    expect(screen.getByLabelText("CA 证书（可选）")).toBeTruthy();
    await select("SASL 认证方式", "PLAIN");
    fireEvent.change(screen.getByLabelText("用户名"), {
      target: { value: "mqtt-user" },
    });
    fireEvent.change(screen.getByLabelText("密码"), {
      target: { value: "new-kafka-secret" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(saved().outbounds[0].config.security).toMatchObject({
      protocol: "sasl_ssl",
      sasl_mechanism: "PLAIN",
      username: "mqtt-user",
      password: "new-kafka-secret",
      ca_pem: null,
    });
    expect(storage).not.toHaveBeenCalled();
    expect(screen.queryByText("new-kafka-secret")).toBeNull();
  });
  it("retains hidden existing credentials and private keys on name-only edits", async () => {
    const cert =
      "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----";
    server({
      schema_version: 2,
      rules: [],
      outbounds: [
        {
          ...kafka,
          config: {
            ...kafka.config,
            security: {
              ...kafka.config.security,
              protocol: "sasl_ssl",
              sasl_mechanism: "SCRAM-SHA-512",
              username: "mqtt-user",
              password: "[redacted]",
              certificate_pem: cert,
              private_key_pem: "[redacted]",
              private_key_password: "[redacted]",
            },
          },
        },
      ],
    });
    render(<RulesPage token="token" expired={() => {}} />);
    await outboundTab();
    fireEvent.click(screen.getByRole("button", { name: "编辑 Kafka stream" }));
    expect(screen.getByLabelText("密码")).toHaveProperty("type", "password");
    expect(screen.getByLabelText("密码")).toHaveProperty("value", "[redacted]");
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Renamed Kafka" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(saved().outbounds[0].config.security).toMatchObject({
      password: "[redacted]",
      private_key_pem: "[redacted]",
      private_key_password: "[redacted]",
    });
  });
  it("preserves advanced tuning on collapse and blocks hidden invalid settings", async () => {
    await freshKafka();
    const advanced = screen.getByRole("button", { name: "高级设置" });
    fireEvent.click(advanced);
    await select("压缩方式", "Zstd");
    fireEvent.change(screen.getByLabelText("发送批次大小（KiB）"), {
      target: { value: 128 },
    });
    fireEvent.change(screen.getByLabelText("Key 字段（可选）"), {
      target: { value: "device_id" },
    });
    fireEvent.change(screen.getByLabelText("发送等待（ms）"), {
      target: { value: 101 },
    });
    fireEvent.click(advanced);
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() =>
      expect(advanced.getAttribute("aria-expanded")).toBe("true"),
    );
    expect(screen.getByRole("alert").textContent).toContain("0–100");
    expect(
      request.mock.calls.filter((c) => c[3]?.method === "PUT"),
    ).toHaveLength(0);
    fireEvent.change(screen.getByLabelText("发送等待（ms）"), {
      target: { value: 2 },
    });
    fireEvent.click(advanced);
    fireEvent.click(screen.getByRole("button", { name: "保存并应用" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(saved().outbounds[0].config).toMatchObject({
      compression: "zstd",
      batch_bytes: 131072,
      key_field: "device_id",
      linger_ms: 2,
    });
  });
  it("escapes Kafka target text and shows actionable errors without raw native details", async () => {
    server();
    request.mockImplementation(async (path) =>
      path.endsWith("config")
        ? ({ schema_version: 2, rules: [], outbounds: [kafka] } as never)
        : ({
            ...status,
            outbounds: [
              {
                id: kafka.id,
                type: "kafka",
                inflight_requests: 0,
                queued: 0,
                durable_queued: 0,
                delivered: 0,
                failed: 1,
                retried: 0,
                dropped: 0,
                last_error: "kafka_auth_failed:private-detail",
              },
            ],
          } as never),
    );
    render(<RulesPage token="token" expired={() => {}} />);
    await outboundTab();
    expect(
      screen.getByText("Kafka 认证失败，请检查用户名、密码和认证方式"),
    ).toBeTruthy();
    expect(screen.queryByText(/private-detail/)).toBeNull();
  });
});
