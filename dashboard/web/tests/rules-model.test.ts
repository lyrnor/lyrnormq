import { describe, it, expect } from "vitest";
import {
  defaultOutbound,
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
