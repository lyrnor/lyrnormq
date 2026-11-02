import { describe, expect, it } from "vitest";
import {
  defaultKafkaOutbound,
  defaultOutbound,
  switchOutboundType,
  validateOutbound,
  validateRulesLimits,
  outboundError,
  type KafkaOutbound,
} from "../src/rules-model";
import {
  changeKafkaProtocol,
  validateKafka,
  defaultKafkaSecurity,
} from "../src/kafka-model";
function valid(): KafkaOutbound {
  const o = defaultKafkaOutbound();
  return {
    ...o,
    id: "kafka-out",
    name: "Kafka",
    config: { ...o.config, brokers: ["localhost:9092"], topic: "events" },
  };
}
describe("Kafka form contract", () => {
  it("uses separate Kafka defaults and resets type-specific fields without changing identity", () => {
    const k = valid();
    expect(k.delivery).toMatchObject({
      concurrency: 128,
      timeout_ms: 30000,
      batch_size: 1,
      batch_wait_ms: 0,
      queue_capacity: 256,
    });
    expect(validateOutbound(k)).toBeNull();
    const w = switchOutboundType(k, "webhook");
    expect(w.id).toBe(k.id);
    expect(w.name).toBe(k.name);
    expect(w.delivery.concurrency).toBe(8);
    expect(w.delivery.timeout_ms).toBe(3000);
    expect(w.config).toEqual({ url: "", headers: {} });
    expect(switchOutboundType(w, "kafka").config).not.toHaveProperty("headers");
    expect(defaultOutbound().delivery.concurrency).toBe(8);
  });
  it("validates bounded host/port targets, IPv6 and literal Kafka topic names", () => {
    const c = valid().config;
    for (const brokers of [
      ["localhost:9092"],
      ["[::1]:9093"],
      ["kafka-1.service:9092", "10.0.0.1:9092"],
    ])
      expect(validateKafka({ ...c, brokers })).toBeNull();
    for (const brokers of [
      [],
      ["http://localhost:9092"],
      ["u:p@host:9092"],
      ["a:0"],
      ["a:65536"],
      [" a:9092"],
      ["[broken]:9092"],
      ["[:::]:9092"],
      ["x".repeat(64) + ":9092"],
      ["kafka..example:9092"],
      ["kafka.example.:9092"],
      Array(17).fill("a:9092"),
    ])
      expect(validateKafka({ ...c, brokers })).not.toBeNull();
    for (const topic of [".", "..", "bad/name", "x".repeat(250), "中文"])
      expect(validateKafka({ ...c, topic })).not.toBeNull();
    expect(validateKafka({ ...c, topic: "..." })).toBeNull();
  });
  it("keeps Kafka concurrency/timeout bounds and fixed per-message policy separate", () => {
    const k = valid();
    for (const concurrency of [1, 128, 512])
      expect(
        validateOutbound({ ...k, delivery: { ...k.delivery, concurrency } }),
      ).toBeNull();
    for (const p of [
      { concurrency: 513 },
      { concurrency: NaN },
      { timeout_ms: 999 },
      { batch_size: 2 },
      { batch_wait_ms: 1 },
    ])
      expect(
        validateOutbound({ ...k, delivery: { ...k.delivery, ...p } }),
      ).not.toBeNull();
    for (const p of [
      { linger_ms: 101 },
      { linger_ms: 1.1 },
      { batch_bytes: 16383 },
      { batch_bytes: 1048577 },
      { key_field: "a\n" },
      { key_field: "中".repeat(43) },
    ])
      expect(validateKafka({ ...k.config, ...p })).not.toBeNull();
  });
  it("preserves redacted SASL and TLS secrets and removes irrelevant values on protocol switches", () => {
    const c = valid().config,
      s = defaultKafkaSecurity("sasl_ssl");
    s.username = "user";
    s.password = "[redacted]";
    expect(validateKafka({ ...c, security: s })).toBeNull();
    expect(
      validateKafka({
        ...c,
        security: { ...s, sasl_mechanism: "PLAIN", password: "" },
      }),
    ).not.toBeNull();
    expect(
      validateKafka({ ...c, security: { ...s, protocol: "plaintext" } }),
    ).not.toBeNull();
    const certificate =
      "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----";
    const tls = {
      ...s,
      ca_pem: certificate,
      certificate_pem: certificate,
      private_key_pem: "[redacted]",
      private_key_password: "[redacted]",
    };
    expect(validateKafka({ ...c, security: tls })).toBeNull();
    expect(
      validateKafka({ ...c, security: { ...tls, private_key_pem: null } }),
    ).not.toBeNull();
    expect(
      validateKafka({ ...c, security: { ...tls, ca_pem: "/etc/ssl/private" } }),
    ).not.toBeNull();
    expect(changeKafkaProtocol(tls, "ssl")).toMatchObject({
      password: null,
      username: null,
      sasl_mechanism: null,
      private_key_pem: "[redacted]",
    });
    expect(changeKafkaProtocol(tls, "plaintext")).toEqual(
      defaultKafkaSecurity(),
    );
  });
  it("bounds enabled Kafka count and whole configuration bytes", () => {
    const k = valid();
    expect(
      validateRulesLimits({
        schema_version: 2,
        rules: [],
        outbounds: Array.from({ length: 9 }, (_, i) => ({
          ...k,
          id: `out-${i}`,
        })),
      }),
    ).toContain("8 个");
    expect(
      validateRulesLimits({
        schema_version: 2,
        rules: [],
        outbounds: [{ ...k, name: "x".repeat(65536) }],
      }),
    ).toContain("64 KiB");
  });
});

it("maps Kafka failures to safe product messages", () => {
  for (const code of [
    "kafka_auth",
    "kafka_tls",
    "kafka_delivery",
    "kafka_queue_full",
    "kafka_event",
    "kafka_key",
    "kafka_closed",
    "kafka_batch",
    "kafka_cancelled",
    "kafka_shutdown",
    "request_too_large",
    "kafka_topic",
  ]) {
    expect(outboundError(code, "kafka")).not.toContain(code);
  }
  expect(
    outboundError("unexpected secret-native-error", "kafka"),
  ).not.toContain("secret-native-error");
});
