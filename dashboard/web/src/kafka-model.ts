export type KafkaProtocol = "plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl";
export interface KafkaSecurity {
  protocol: KafkaProtocol;
  sasl_mechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | null;
  username: string | null;
  password: string | null;
  ca_pem: string | null;
  certificate_pem: string | null;
  private_key_pem: string | null;
  private_key_password: string | null;
}
export interface KafkaConfig {
  brokers: string[];
  topic: string;
  key_field: string | null;
  security: KafkaSecurity;
  compression: "none" | "lz4" | "zstd";
  linger_ms: number;
  batch_bytes: number;
}
export function defaultKafkaSecurity(
  protocol: KafkaProtocol = "plaintext",
): KafkaSecurity {
  const sasl = protocol.startsWith("sasl_");
  return {
    protocol,
    sasl_mechanism: sasl ? "SCRAM-SHA-256" : null,
    username: null,
    password: null,
    ca_pem: null,
    certificate_pem: null,
    private_key_pem: null,
    private_key_password: null,
  };
}
export const defaultKafkaConfig = (): KafkaConfig => ({
  brokers: [],
  topic: "",
  key_field: null,
  security: defaultKafkaSecurity(),
  compression: "lz4",
  linger_ms: 1,
  batch_bytes: 65536,
});
const bytes = (s: string) => new TextEncoder().encode(s).length;
const present = (s: string | null | undefined) => s !== null && s !== undefined;
const text = (s: string | null, limit: number, empty = false) =>
  typeof s === "string" &&
  (empty || s.length > 0) &&
  !s.includes("\0") &&
  bytes(s) <= limit;
function brokerValid(value: string): boolean {
  if (
    !value ||
    bytes(value) > 255 ||
    /[^\x21-\x7e]/.test(value) ||
    /[\/@?#]/.test(value)
  )
    return false;
  const match =
    /^(\[[0-9a-fA-F:.]+\]|[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*):(\d+)$/.exec(
      value,
    );
  if (!match || Number(match[2]) < 1 || Number(match[2]) > 65535) return false;
  if (match[1].startsWith("[")) {
    try {
      const url = new URL(`http://${value}`);
      return url.hostname.startsWith("[");
    } catch {
      return false;
    }
  }
  return (
    match[1].length <= 253 &&
    match[1]
      .split(".")
      .every((label) => label.length >= 1 && label.length <= 63)
  );
}
function pem(value: string | null, kind: "certificate" | "key"): boolean {
  if (!text(value, 16384)) return false;
  if (value === "[redacted]" && kind === "key") return true;
  return kind === "certificate"
    ? /^\s*-----BEGIN CERTIFICATE-----[\s\S]+-----END CERTIFICATE-----\s*$/.test(
        value!,
      )
    : /^\s*-----BEGIN (?:RSA |EC |ENCRYPTED )?PRIVATE KEY-----[\s\S]+-----END (?:RSA |EC |ENCRYPTED )?PRIVATE KEY-----\s*$/.test(
        value!,
      );
}
export function validateKafka(c: KafkaConfig): string | null {
  if (
    !Array.isArray(c.brokers) ||
    c.brokers.length < 1 ||
    c.brokers.length > 16 ||
    c.brokers.some((b) => typeof b !== "string" || !brokerValid(b))
  )
    return "Broker 地址需为 1–16 个主机:端口或 [IPv6]:端口，不能包含路径或凭据";
  if (
    typeof c.topic !== "string" ||
    !/^[A-Za-z0-9._-]{1,249}$/.test(c.topic) ||
    c.topic === "." ||
    c.topic === ".."
  )
    return "Kafka Topic 需为 1–249 位字母、数字、点、下划线或连字符，且不能为 . 或 ..";
  const tuning = validateKafkaAdvanced(c);
  if (tuning) return tuning;
  const s = c.security;
  if (
    !s ||
    !["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"].includes(s.protocol)
  )
    return "请选择有效的安全连接方式";
  const sasl = s.protocol.startsWith("sasl_"),
    tls = s.protocol === "ssl" || s.protocol === "sasl_ssl";
  if (sasl) {
    if (
      !["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(
        s.sasl_mechanism ?? "",
      ) ||
      !text(s.username, 1024) ||
      !text(s.password, 1024)
    )
      return "请填写 SASL 认证方式、用户名和密码";
  } else if ([s.sasl_mechanism, s.username, s.password].some(present))
    return "当前安全连接方式不使用 SASL 凭据";
  if (
    !tls &&
    [
      s.ca_pem,
      s.certificate_pem,
      s.private_key_pem,
      s.private_key_password,
    ].some(present)
  )
    return "当前连接方式不使用 TLS 证书";
  if (tls) {
    if (present(s.ca_pem) && !pem(s.ca_pem, "certificate"))
      return "请填写有效的 CA 证书 PEM，最多 16 KiB";
    if (present(s.certificate_pem) !== present(s.private_key_pem))
      return "客户端证书和私钥需同时填写";
    if (present(s.certificate_pem) && !pem(s.certificate_pem, "certificate"))
      return "请填写有效的客户端证书 PEM，最多 16 KiB";
    if (present(s.private_key_pem) && !pem(s.private_key_pem, "key"))
      return "请填写有效的客户端私钥 PEM，最多 16 KiB";
    if (
      present(s.private_key_password) &&
      (!present(s.private_key_pem) || !text(s.private_key_password, 1024))
    )
      return "私钥口令需要已配置的私钥，最多 1024 字节";
  }
  return null;
}

export function changeKafkaProtocol(
  old: KafkaSecurity,
  protocol: KafkaProtocol,
): KafkaSecurity {
  const next = defaultKafkaSecurity(protocol),
    tls = (p: KafkaProtocol) => p === "ssl" || p === "sasl_ssl";
  if (tls(old.protocol) && tls(protocol)) {
    next.ca_pem = old.ca_pem;
    next.certificate_pem = old.certificate_pem;
    next.private_key_pem = old.private_key_pem;
    next.private_key_password = old.private_key_password;
  }
  if (old.protocol.startsWith("sasl_") && protocol.startsWith("sasl_")) {
    next.sasl_mechanism = old.sasl_mechanism;
    next.username = old.username;
    next.password = old.password;
  }
  return next;
}

export function validateKafkaAdvanced(c: KafkaConfig): string | null {
  if (
    c.key_field !== null &&
    c.key_field !== undefined &&
    (typeof c.key_field !== "string" ||
      !c.key_field.length ||
      bytes(c.key_field) > 128 ||
      /[\x00-\x1f\x7f]/.test(c.key_field))
  )
    return "Key 字段不能为空，最多 128 字节且不能含控制字符";
  if (!["none", "lz4", "zstd"].includes(c.compression))
    return "请选择有效的压缩方式";
  if (!Number.isInteger(c.linger_ms) || c.linger_ms < 0 || c.linger_ms > 100)
    return "发送等待必须为 0–100 ms 的整数";
  if (
    !Number.isInteger(c.batch_bytes) ||
    c.batch_bytes < 16384 ||
    c.batch_bytes > 1048576
  )
    return "发送批次大小需为 16–1024 KiB";
  return null;
}
