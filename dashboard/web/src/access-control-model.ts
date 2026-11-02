export type AccessStage = "authentication" | "authorization";
export type ServiceAuth =
  | { type: "none" | "headers" }
  | { type: "bearer"; token: string }
  | { type: "basic"; username: string; password: string };
export interface BackendConfig {
  type: "http";
  enabled: boolean;
  name: string;
  url: string;
  service_auth: ServiceAuth;
  headers: Record<string, string>;
  timeout_ms: number;
  max_concurrency: number;
  allow_http: boolean;
  tls: {
    ca_pem: string | null;
    certificate_pem: string | null;
    private_key_pem: string | null;
  };
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
  if (c.url || c.enabled || probe) {
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
  const t = c.tls;
  if (!!t.certificate_pem !== !!t.private_key_pem)
    return "客户端证书和私钥需要成对配置。";
  for (const [kind, value] of Object.entries(t)) {
    if (!value) continue;
    if (kind === "private_key_pem" && value === "[redacted]") continue;
    if (utf8Bytes(value) > 16384 || !value.includes("-----BEGIN "))
      return "请选择有效的 PEM 文件，每个文件最多 16 KiB。";
    if (kind === "private_key_pem" && value.includes("ENCRYPTED"))
      return "目前仅支持未加密的客户端私钥。";
  }
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
