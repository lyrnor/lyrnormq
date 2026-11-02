export type AccessStage = "authentication" | "authorization";
export type ServiceAuth =
  | { type: "none" | "headers" }
  | { type: "bearer"; token: string }
  | { type: "basic"; username: string; password: string };
export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  tls_mode: "verify_full" | "disable";
  ca_pem: string | null;
  max_connections: number;
  query: string;
}
export function postgresDefaults(authorization: boolean): PostgresConfig {
  return {
    host: "localhost",
    port: 5432,
    database: "mqtt",
    username: "mqtt",
    password: "",
    tls_mode: "verify_full",
    ca_pem: null,
    max_connections: 8,
    query: authorization
      ? "SELECT effect, topic_filter, priority FROM mqtt_acl WHERE username = $1 AND (client_id IS NULL OR client_id = $2) AND action = $3 AND enabled = TRUE"
      : "SELECT password_hash FROM mqtt_users WHERE username = $1 AND enabled = TRUE",
  };
}
export interface BackendConfig {
  type: "http" | "postgresql";
  postgres?: PostgresConfig;
  enabled: boolean;
  name: string;
  url: string;
  service_auth: ServiceAuth;
  headers: Record<string, string>;
  timeout_ms: number;
  max_concurrency: number;
  allow_http: boolean;
}
export interface AuthorizationConfig extends BackendConfig {
  cache_ttl_seconds: number;
}
export interface AccessConfig {
  schema_version: 1;
  authentication: BackendConfig;
  authorization: AuthorizationConfig;
}
export interface StageStatus {
  pool_connections?: number;
  pool_idle_connections?: number;
  enabled: boolean;
  requests_total: number;
  allowed_total: number;
  denied_total: number;
  errors_total: number;
  last_result: "allow" | "deny" | "unavailable" | null;
  last_http_status: number | null;
  last_latency_ms: number | null;
}
export interface CacheStatus {
  entries: number;
  bytes: number;
  hits: number;
  misses: number;
  inflight: number;
}
export interface AccessStatus {
  postgresql_available?: boolean;
  authentication: StageStatus;
  authorization: StageStatus;
  cache: CacheStatus;
}
export interface ProbeResult {
  result: "allow" | "deny" | "unavailable";
  http_status: number | null;
  elapsed_ms: number;
  error_code: string | null;
}
export interface HeaderEntry {
  name: string;
  value: string;
}
export interface AccessDraft {
  value: BackendConfig | AuthorizationConfig;
  headers: HeaderEntry[];
}
export const ACCESS_PATH = "/api/v1/access-control";
export const utf8Bytes = (s: string) => new TextEncoder().encode(s).length;
export function accessDraft(
  value: BackendConfig | AuthorizationConfig,
): AccessDraft {
  return {
    value: structuredClone(value),
    headers: Object.entries(value.headers).map(([name, value]) => ({
      name,
      value,
    })),
  };
}
export function draftValue(
  draft: AccessDraft,
): BackendConfig | AuthorizationConfig {
  return {
    ...draft.value,
    headers: Object.fromEntries(
      draft.headers.map(({ name, value }) => [name, value]),
    ),
  };
}
export function changeServiceAuth(type: ServiceAuth["type"]): ServiceAuth {
  if (type === "bearer") return { type, token: "" };
  if (type === "basic") return { type, username: "", password: "" };
  return { type };
}
const control = /[\u0000-\u001f\u007f-\u009f]/;
const headerName = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
const forbiddenHeaders = new Set([
  "host",
  "connection",
  "content-length",
  "transfer-encoding",
  "content-type",
  "accept",
  "accept-encoding",
  "upgrade",
  "te",
  "trailer",
  "proxy-authorization",
  "proxy-connection",
  "cookie",
]);
export function validateAccessDraft(
  draft: AccessDraft,
  probe = false,
): string | null {
  const c = draft.value;
  if (!c.name.trim() || utf8Bytes(c.name) > 128 || control.test(c.name))
    return "名称不能为空，且最多 128 字节。";
  if (c.type === "http" && (c.url || c.enabled || probe)) {
    let url: URL;
    try {
      url = new URL(c.url);
    } catch {
      return "请输入完整的 HTTP 或 HTTPS 地址。";
    }
    if (
      utf8Bytes(c.url) > 2048 ||
      !["http:", "https:"].includes(url.protocol) ||
      !url.hostname
    )
      return "请输入有效地址，长度最多 2048 字节。";
    if (
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      /[\s\u0000-\u001f]/.test(c.url)
    )
      return "地址不能包含凭据、查询参数、片段或空白。";
    if (url.protocol === "http:" && !c.allow_http)
      return "明文 HTTP 需先在高级设置中明确允许。";
  }
  if (
    !Number.isInteger(c.timeout_ms) ||
    c.timeout_ms < 100 ||
    c.timeout_ms > 5000
  )
    return "请求超时应为 100–5000 ms。";
  if (
    !Number.isInteger(c.max_concurrency) ||
    c.max_concurrency < 1 ||
    c.max_concurrency > 256
  )
    return "最大并发应为 1–256。";
  if (
    "cache_ttl_seconds" in c &&
    (!Number.isInteger(c.cache_ttl_seconds) ||
      c.cache_ttl_seconds < 0 ||
      c.cache_ttl_seconds > 300)
  )
    return "缓存时间应为 0–300 秒，0 表示不缓存。";
  if (c.type === "postgresql") {
    const p = c.postgres;
    if (
      !p ||
      !p.host ||
      utf8Bytes(p.host) > 253 ||
      /[\s/]/.test(p.host) ||
      control.test(p.host) ||
      !p.database ||
      utf8Bytes(p.database) > 128 ||
      control.test(p.database) ||
      !p.username ||
      utf8Bytes(p.username) > 128 ||
      control.test(p.username)
    )
      return "请填写有效的 PostgreSQL 主机、数据库和用户名。";
    if (!Number.isInteger(p.port) || p.port < 1 || p.port > 65535)
      return "数据库端口应为 1–65535。";
    if (
      !Number.isInteger(p.max_connections) ||
      p.max_connections < 1 ||
      p.max_connections > 32
    )
      return "连接池上限应为 1–32。";
    if (utf8Bytes(p.password) > 1024 || p.password.includes("\0"))
      return "数据库密码最多 1024 字节。";
    if (!p.query.trim() || utf8Bytes(p.query) > 16384 || p.query.includes("\0"))
      return "请填写查询 SQL，最多 16384 字节。";
    if (!["verify_full", "disable"].includes(p.tls_mode))
      return "请选择有效的 TLS 模式。";
    if (p.ca_pem !== null && (!p.ca_pem || utf8Bytes(p.ca_pem) > 16384))
      return "CA 证书最多 16384 字节。";
    return null;
  }
  const s = c.service_auth;
  if (
    s.type === "bearer" &&
    (!s.token || utf8Bytes(s.token) > 1024 || /[^\x21-\x7e]/.test(s.token))
  )
    return "请填写有效的服务 Token，最多 1024 字节。";
  if (
    s.type === "basic" &&
    (!s.username ||
      utf8Bytes(s.username) > 256 ||
      /[:\u0000-\u001f\u007f]/.test(s.username) ||
      !s.password ||
      utf8Bytes(s.password) > 1024 ||
      /[\r\n\u0000]/.test(s.password))
  )
    return "请填写有效的服务用户名和密码。";
  if (draft.headers.length > 32) return "请求头最多 32 项。";
  const names = new Set<string>();
  let headerBytes = 0;
  for (const h of draft.headers) {
    const key = h.name.toLowerCase();
    if (!headerName.test(h.name) || forbiddenHeaders.has(key))
      return "请求头名称无效，或属于系统管理的请求头。";
    if (names.has(key)) return "请求头名称不能重复。";
    if (key === "authorization" && s.type !== "headers")
      return "自定义 Authorization 请求头需要选择“自定义请求头”鉴权。";
    if (/[\r\n\u0000]/.test(h.value)) return "请求头值不能包含换行或空字符。";
    names.add(key);
    headerBytes += utf8Bytes(h.name) + utf8Bytes(h.value);
  }
  if (headerBytes > 16384) return "请求头内容过大，请减少后重试。";
  return null;
}
export function stageHealth(status: StageStatus | undefined): string {
  if (!status) return "状态待更新";
  if (!status.enabled) return "未启用";
  if (!status.requests_total || !status.last_result) return "暂无调用结果";
  if (status.last_result === "unavailable") return "服务异常";
  return status.last_result === "allow" ? "最近请求通过" : "最近请求被拒绝";
}
export function accessError(code?: string): string | null {
  if (!code) return null;
  const messages: Record<string, string> = {
    access_invalid_config: "配置无效，请检查地址、鉴权和参数范围。",
    access_invalid_request: "请求参数无效，请检查填写内容。",
    access_unavailable: "认证与授权服务暂不可用，请稍后重试。",
    access_busy: "认证与授权服务繁忙，请稍后重试。",
    access_storage: "配置暂时无法保存，请稍后重试。",
    access_stopped: "认证与授权服务暂不可用，请联系管理员。",
  };
  return messages[code] ?? null;
}
