import { describe, it, expect, afterEach, vi } from "vitest";
import {
  defaultOutbound,
  createResourceId,
  validateName,
  deletionError,
  testEvent,
  validateOutbound,
  validateWebhook,
  validateIdentity,
  validateSql,
  type RulesConfig,
} from "../src/rules-model";
const connector = {
  id: "hook",
  name: "Webhook",
  url: "https://example.com/events",
  headers: { Authorization: "[redacted]" },
};
const action = {
  ...defaultOutbound(),
  id: "send",
  name: "Send",
  config: { url: connector.url, headers: connector.headers },
};
const config: RulesConfig = {
  schema_version: 2,
  outbounds: [action],
  rules: [
    {
      id: "hot",
      name: "High temp",
      sql: 'SELECT * FROM "devices/#"',
      enabled: true,
      outbound_ids: ["send"],
    },
  ],
};
describe("rule form contract validation", () => {
  it("defaults new outbounds to one event without batching delay", () => {
    expect(defaultOutbound().delivery).toEqual({
      batch_size: 1,
      batch_wait_ms: 0,
      concurrency: 8,
      queue_capacity: 256,
      timeout_ms: 3000,
      max_attempts: 3,
      retry_base_ms: 200,
    });
  });
  it("validates IDs and UTF8 byte limits", () => {
    expect(validateIdentity("valid-id_1", "中文")).toBeNull();
    for (const id of ["", "spaces bad", "../other", "x".repeat(65)])
      expect(validateIdentity(id, "Name")).not.toBeNull();
    expect(validateIdentity("ok", "中".repeat(43))).not.toBeNull();
    expect(validateSql("中".repeat(2731))).not.toBeNull();
    expect(validateSql(" ")).not.toBeNull();
  });
  it("requires allowed URL shape and keeps redacted credentials roundtrippable", () => {
    expect(validateWebhook(connector)).toBeNull();
    for (const url of [
      "file:///tmp/secret",
      "https://a:b@example.com",
      "https://example.com/?token=x",
      "https://example.com/#secret",
      "not-url",
    ])
      expect(validateWebhook({ ...connector, url })).not.toBeNull();
  });
  it("rejects header injection, system fields and case-insensitive duplicates", () => {
    for (const headers of [
      { Host: "evil" },
      { "Content-Type": "text/plain" },
      { Authorization: "a\r\nB:c" },
      { Bad: "x", bad: "y" },
      { "not valid": "x" },
    ] as Record<string, string>[])
      expect(validateWebhook({ ...connector, headers })).not.toBeNull();
    expect(
      validateWebhook({
        ...connector,
        headers: { Authorization: "new-secret" },
      }),
    ).toBeNull();
  });
  it("checks action references and all resource limits", () => {
    expect(validateOutbound(action)).toBeNull();
    expect(
      validateOutbound({ ...action, type: "unknown" as "webhook" }),
    ).not.toBeNull();
    for (const patch of [
      { concurrency: 0 },
      { concurrency: 17 },
      { concurrency: 1.5 },
      { batch_size: 501 },
      { batch_wait_ms: 1001 },
      { queue_capacity: 0 },
      { timeout_ms: 99 },
      { max_attempts: 6 },
      { retry_base_ms: NaN },
    ])
      expect(
        validateOutbound({
          ...action,
          delivery: { ...action.delivery, ...patch },
        }),
      ).not.toBeNull();
  });
  it("accepts sequential and maximum request concurrency", () => {
    for (const concurrency of [1, 16])
      expect(
        validateOutbound({
          ...action,
          delivery: { ...action.delivery, concurrency },
        }),
      ).toBeNull();
  });
  it("blocks deletion of referenced actions and connectors", () => {
    expect(deletionError("outbounds", "send", config)).toContain("绑定");
    expect(deletionError("rules", "hot", config)).toBeNull();
  });
  it("encodes a distinct sample event as UTF8 bytes without requiring JSON", () => {
    const event = testEvent("devices/one/data", "test-device", "中文 raw");
    expect(new TextDecoder().decode(Uint8Array.from(event.payload))).toBe(
      "中文 raw",
    );
    expect(event.qos).toBe(0);
    expect(event.timestamp_ms).toBeGreaterThan(0);
    expect(() => testEvent("devices/#", "test", "x")).toThrow();
    expect(() => testEvent("a", "", "x")).toThrow();
  });
});

afterEach(() => vi.unstubAllGlobals());
describe("automatic internal identifiers", () => {
  it("generates bounded distinct namespaced identifiers with the Crypto receiver", () => {
    let counter = 0;
    const source = {
      randomUUID() {
        expect(this).toBe(source);
        return `00000000-0000-4000-8000-${String(++counter).padStart(12, "0")}`;
      },
    };
    vi.stubGlobal("crypto", source);
    const first = createResourceId("rules", []);
    const second = createResourceId("rules", [{ id: first }]);
    expect(first).not.toBe(second);
    expect(validateIdentity(first, "名称")).toBeNull();
    expect(createResourceId("outbounds", [])).toMatch(/^out_/);
    expect(validateName(" ")).not.toBeNull();
  });
  it("uses getRandomValues when randomUUID is unavailable", () => {
    const source = {
      getRandomValues<T extends Uint8Array>(bytes: T): T {
        expect(this).toBe(source);
        bytes.fill(0);
        return bytes;
      },
    };
    vi.stubGlobal("crypto", source);
    expect(createResourceId("rules", [])).toBe(
      "rule_00000000-0000-4000-8000-000000000000",
    );
  });
  it("retries collisions with a strict bound and reports unavailable crypto safely", () => {
    const uuid = "00000000-0000-4000-8000-000000000000";
    const randomUUID = vi.fn(() => uuid);
    vi.stubGlobal("crypto", { randomUUID });
    expect(() => createResourceId("rules", [{ id: `rule_${uuid}` }])).toThrow(
      "暂时无法创建",
    );
    expect(randomUUID).toHaveBeenCalledTimes(8);
    randomUUID.mockReturnValueOnce("11111111-1111-4111-8111-111111111111");
    expect(createResourceId("rules", [{ id: `rule_${uuid}` }])).toContain(
      "11111111",
    );
    vi.stubGlobal("crypto", undefined);
    expect(() => createResourceId("rules", [])).toThrow("暂时无法创建");
  });
});
