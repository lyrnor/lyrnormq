import {
  defaultKafkaConfig,
  validateKafka,
  type KafkaConfig,
} from "./kafka-model";
export type { KafkaConfig, KafkaSecurity, KafkaProtocol } from "./kafka-model";
export interface RuleDefinition {
  id: string;
  name: string;
  sql: string;
  enabled: boolean;
  outbound_ids: string[];
}
export interface WebhookConfig {
  url: string;
  headers: Record<string, string>;
}
export interface DeliveryPolicy {
  concurrency: number;
  batch_size: number;
  batch_wait_ms: number;
  queue_capacity: number;
  timeout_ms: number;
  max_attempts: number;
  retry_base_ms: number;
}
interface OutboundBase {
  id: string;
  name: string;
  enabled: boolean;
  delivery: DeliveryPolicy;
}
export interface WebhookOutbound extends OutboundBase {
  type: "webhook";
  config: WebhookConfig;
}
export interface KafkaOutbound extends OutboundBase {
  type: "kafka";
  config: KafkaConfig;
}
export type Outbound = WebhookOutbound | KafkaOutbound;
export const defaultKafkaOutbound = (): KafkaOutbound => ({
  id: "",
  name: "",
  enabled: true,
  type: "kafka",
  config: defaultKafkaConfig(),
  delivery: {
    concurrency: 128,
    batch_size: 1,
    batch_wait_ms: 0,
    queue_capacity: 256,
    timeout_ms: 30000,
    max_attempts: 3,
    retry_base_ms: 200,
  },
});
export function switchOutboundType(
  outbound: Outbound,
  type: Outbound["type"],
): Outbound {
  if (outbound.type === type) return outbound;
  return {
    ...(type === "kafka" ? defaultKafkaOutbound() : defaultOutbound()),
    id: outbound.id,
    name: outbound.name,
    enabled: outbound.enabled,
  };
}
export function outboundTarget(outbound: Outbound): string {
  return outbound.type === "kafka"
    ? `Kafka · ${outbound.config.brokers.join(", ")} / ${outbound.config.topic}`
    : `Webhook · ${outbound.config.url}`;
}
export function outboundError(code: string | null, type: string): string {
  if (!code) return "—";
  if (type !== "kafka") return code;
  const kafkaErrors: Record<string, string> = {
    kafka_delivery: "Kafka 投递未成功，请检查服务和重试设置",
    kafka_queue_full: "发送目标暂时繁忙，请稍后重试",
    kafka_event: "消息格式无法发送，请检查规则输出",
    kafka_closed: "出站已停止，请检查运行状态",
    kafka_batch: "发送配置无效，请检查后重试",
    kafka_cancelled: "发送已暂停，请检查出站状态",
    kafka_shutdown: "出站暂时无法恢复，请检查运行状态",
  };
  if (kafkaErrors[code]) return kafkaErrors[code];
  if (/auth|sasl|credential/i.test(code))
    return "Kafka 认证失败，请检查用户名、密码和认证方式";
  if (/tls|ssl|certificate/i.test(code))
    return "Kafka 安全连接失败，请检查证书和服务器地址";
  if (/size|large/i.test(code)) return "消息超出目标允许的大小";
  if (/key/i.test(code)) return "消息 Key 字段不存在或不是字符串";
  if (/topic/i.test(code)) return "Kafka Topic 不可用或没有写入权限";
  if (/timeout|transport|network|connect/i.test(code))
    return "Kafka 暂时不可达或投递超时";
  return "Kafka 投递失败，请检查目标配置和运行状态";
}
export interface RulesConfig {
  schema_version: 2;
  outbounds: Outbound[];
  rules: RuleDefinition[];
}
export interface OutboundStatus {
  inflight_requests: number;
  type: string;
  id: string;
  queued: number;
  durable_queued?: number;
  delivered: number;
  failed: number;
  retried: number;
  dropped: number;
  last_error: string | null;
}
export interface RulesStatus {
  inflight_requests: number;
  running: boolean;
  mode: "best_effort";
  queued_events: number;
  queued_bytes: number;
  durable_queue?: {
    pending_records: number;
    pending_results?: number;
    pending_bytes: number;
    disk_bytes: number;
    max_bytes: number;
    recovered_records: number;
  };
  received: number;
  matched: number;
  dropped: number;
  errors: number;
  outbounds: OutboundStatus[];
}
export interface RuleEvent {
  topic: string;
  payload: number[];
  client_id: string;
  qos: number;
  retain: boolean;
  timestamp_ms: number;
}
export const SQL_EXAMPLE = `SELECT client_id, topic AS source_topic, payload.temperature AS temperature\nFROM "devices/+/telemetry"\nWHERE payload.temperature >= 80`;
export const defaultOutbound = (): WebhookOutbound => ({
  id: "",
  name: "",
  enabled: true,
  type: "webhook",
  config: { url: "", headers: {} },
  delivery: {
    concurrency: 8,
    batch_size: 1,
    batch_wait_ms: 0,
    queue_capacity: 256,
    timeout_ms: 3000,
    max_attempts: 3,
    retry_base_ms: 200,
  },
});
/** Generate once when a form opens; persisted IDs remain stable across edits. */
export function createResourceId(
  kind: "rules" | "outbounds",
  existing: readonly { id: string }[],
): string {
  const ids = new Set(existing.map((item) => item.id));
  for (let attempt = 0; attempt < 8; attempt++) {
    const crypto = globalThis.crypto;
    let uuid: string;
    if (typeof crypto?.randomUUID === "function") uuid = crypto.randomUUID();
    else {
      if (typeof crypto?.getRandomValues !== "function")
        throw new Error("暂时无法创建，请稍后重试");
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      const hex = Array.from(bytes, (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("");
      uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    }
    const id = `${kind === "rules" ? "rule" : "out"}_${uuid}`;
    if (/^[A-Za-z0-9_-]{1,64}$/.test(id) && !ids.has(id)) return id;
  }
  throw new Error("暂时无法创建，请稍后重试");
}
const encoder = new TextEncoder();
export function validateName(name: string): string | null {
  return !name.trim() || encoder.encode(name).length > 128
    ? "名称不能为空，且最多 128 字节"
    : null;
}
export function validateIdentity(id: string, name: string): string | null {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(id))
    return "ID 需为 1–64 位字母、数字、下划线或连字符";
  return validateName(name);
}
export function validateSql(sql: string): string | null {
  return !sql.trim() || encoder.encode(sql).length > 8192
    ? "SQL 不能为空，且最多 8 KiB"
    : null;
}
export function validateWebhook(connector: WebhookConfig): string | null {
  try {
    const url = new URL(connector.url);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      return "Webhook 仅支持 HTTP/HTTPS，地址不能含凭据、查询参数或片段；凭据请放请求头";
  } catch {
    return "请输入有效的 Webhook 地址";
  }
  const headers = Object.entries(connector.headers);
  if (
    headers.length > 16 ||
    encoder.encode(headers.map(([k, v]) => k + v).join("")).length > 8192
  )
    return "请求头最多 16 项、合计 8 KiB";
  const forbidden = new Set([
    "host",
    "content-length",
    "content-type",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "upgrade",
    "te",
    "trailer",
    "proxy-authenticate",
    "proxy-authorization",
  ]);
  const seen = new Set<string>();
  for (const [key, value] of headers) {
    const normalized = key.toLowerCase();
    if (
      !/^[!#$%&'*+.^_`|~0-9a-z-]+$/i.test(key) ||
      forbidden.has(normalized) ||
      /[\r\n]/.test(value) ||
      seen.has(normalized)
    )
      return "请求头名称无效、重复或包含系统保留字段；值不能含换行";
    seen.add(normalized);
  }
  return null;
}
export function validateOutbound(outbound: Outbound): string | null {
  const identity = validateIdentity(outbound.id, outbound.name);
  if (identity) return identity;
  if (outbound.type !== "webhook" && outbound.type !== "kafka")
    return "暂不支持此出站类型";
  const destination =
    outbound.type === "kafka"
      ? validateKafka(outbound.config)
      : validateWebhook(outbound.config);
  if (destination) return destination;
  return validateDeliveryPolicy(outbound.delivery, outbound.type);
}
export function validateDeliveryPolicy(
  action: DeliveryPolicy,
  type: Outbound["type"] = "webhook",
): string | null {
  if (
    type === "kafka" &&
    (action.batch_size !== 1 || action.batch_wait_ms !== 0)
  )
    return "Kafka 投递参数无效，请重新选择出站类型";
  const bounds: [number, number, number, string][] = [
    [
      action.concurrency,
      1,
      type === "kafka" ? 512 : 16,
      type === "kafka" ? "并发发送数" : "并发请求数",
    ],
    [action.batch_size, 1, 500, "批量条数"],
    [action.batch_wait_ms, 0, 1000, "批量等待"],
    [action.queue_capacity, 1, 4096, "队列容量"],
    [
      action.timeout_ms,
      type === "kafka" ? 1000 : 100,
      30000,
      type === "kafka" ? "投递超时" : "请求超时",
    ],
    [action.max_attempts, 1, 5, "最多尝试次数"],
    [action.retry_base_ms, 10, 10000, "重试基础间隔"],
  ];
  for (const [value, min, max, label] of bounds)
    if (!Number.isInteger(value) || value < min || value > max)
      return `${label}必须为 ${min}–${max} 的整数`;
  return null;
}
export function deletionError(
  kind: "outbounds" | "rules",
  id: string,
  config: RulesConfig,
): string | null {
  if (
    kind === "outbounds" &&
    config.rules.some((rule) => rule.outbound_ids.includes(id))
  )
    return "出站仍被规则引用，请先解除绑定";
  return null;
}
export function testEvent(
  topic: string,
  clientId: string,
  payload: string,
): RuleEvent {
  if (
    !topic ||
    topic.includes("+") ||
    topic.includes("#") ||
    topic.includes("\0") ||
    encoder.encode(topic).length > 65535
  )
    throw new Error("测试 Topic 不能为空，不能包含通配符或 NUL");
  if (!clientId || encoder.encode(clientId).length > 65535)
    throw new Error("测试 Client ID 不能为空或过长");
  return {
    topic,
    client_id: clientId,
    payload: Array.from(encoder.encode(payload)),
    qos: 0,
    retain: false,
    timestamp_ms: Date.now(),
  };
}

export function isSupportedOutbound(value: unknown): value is Outbound {
  if (!value || typeof value !== "object") return false;
  const o = value as Outbound;
  if (!o.config || typeof o.config !== "object") return false;
  if (o.type === "webhook")
    return (
      typeof o.config.url === "string" &&
      !!o.config.headers &&
      typeof o.config.headers === "object"
    );
  return (
    o.type === "kafka" &&
    Array.isArray(o.config.brokers) &&
    o.config.brokers.every((b) => typeof b === "string") &&
    typeof o.config.topic === "string" &&
    !!o.config.security &&
    typeof o.config.security.protocol === "string"
  );
}
export function validateRulesLimits(config: RulesConfig): string | null {
  if (config.outbounds.length > 64 || config.rules.length > 128)
    return "规则或出站数量超出上限";
  if (
    config.outbounds.filter((o) => o.type === "kafka" && o.enabled).length > 8
  )
    return "最多同时启用 8 个 Kafka 出站";
  if (encoder.encode(JSON.stringify(config)).length > 64 * 1024)
    return "配置总大小不能超过 64 KiB，请缩减证书或配置内容";
  return null;
}
