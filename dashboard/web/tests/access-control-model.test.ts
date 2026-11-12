import { describe, expect, it } from "vitest";
import {
  accessDraft,
  postgresDefaults,
  redisDefaults,
  changeServiceAuth,
  draftValue,
  stageHealth,
  validateAccessDraft,
  type BackendConfig,
  type StageStatus,
} from "../src/access-control-model";
const endpoint: BackendConfig = {
  type: "http",
  enabled: false,
  name: "HTTP 认证",
  url: "",
  service_auth: { type: "none" },
  headers: {},
  timeout_ms: 1000,
  max_concurrency: 128,
  allow_http: false,
};
const stage: StageStatus = {
  enabled: true,
  requests_total: 0,
  allowed_total: 0,
  denied_total: 0,
  errors_total: 0,
  last_result: null,
  last_http_status: null,
  last_latency_ms: null,
};
describe("HTTP access-control form validation", () => {
  it("supports disabled defaults but requires a complete secure target for enable/probe", () => {
    const draft = accessDraft(endpoint);
    expect(validateAccessDraft(draft)).toBeNull();
    expect(validateAccessDraft(draft, true)).toContain("地址");
    draft.value.enabled = true;
    expect(validateAccessDraft(draft)).toContain("地址");
    for (const url of [
      "https://u:p@example.com/check",
      "https://example.com/check?token=x",
      "https://example.com/#frag",
      "ftp://example.com",
      "https://exa mple.com",
    ]) {
      draft.value.url = url;
      expect(validateAccessDraft(draft)).not.toBeNull();
    }
    draft.value.url = "http://127.0.0.1:9000/check";
    expect(validateAccessDraft(draft)).toContain("明文");
    draft.value.allow_http = true;
    expect(validateAccessDraft(draft)).toBeNull();
  });
  it("retains redacted credentials and rejects header ambiguity/injection before building a map", () => {
    const draft = accessDraft({
      ...endpoint,
      headers: { "X-Api-Key": "[redacted]" },
      service_auth: { type: "bearer", token: "[redacted]" },
    });
    expect(validateAccessDraft(draft)).toBeNull();
    expect(draftValue(draft).headers).toEqual({ "X-Api-Key": "[redacted]" });
    draft.headers.push({ name: "x-api-key", value: "new" });
    expect(validateAccessDraft(draft)).toContain("重复");
    draft.headers = [{ name: "Authorization", value: "x" }];
    expect(validateAccessDraft(draft)).toContain("Authorization");
    for (const name of [
      "Host",
      "Content-Type",
      "Transfer-Encoding",
      "Content-Length",
      "Cookie",
      "bad name",
    ]) {
      draft.headers = [{ name, value: "value" }];
      expect(validateAccessDraft(draft)).not.toBeNull();
    }
    draft.value.service_auth = { type: "none" };
    draft.headers = [{ name: "Authorization", value: "token" }];
    expect(validateAccessDraft(draft)).toContain("Authorization");
    draft.value.service_auth = { type: "headers" };
    expect(validateAccessDraft(draft)).toBeNull();
    draft.headers = [{ name: "X-Key", value: "x\r\nInjected:y" }];
    expect(validateAccessDraft(draft)).toContain("换行");
  });
  it("bounds numeric values and accepts HTTPS without certificate options", () => {
    const draft = accessDraft(endpoint);
    for (const timeout of [99, 5001, NaN, 100.5]) {
      draft.value.timeout_ms = timeout;
      expect(validateAccessDraft(draft)).toContain("超时");
    }
    draft.value.timeout_ms = 1000;
    draft.value.max_concurrency = 257;
    expect(validateAccessDraft(draft)).toContain("并发");
    draft.value.max_concurrency = 128;
    draft.value.enabled = true;
    draft.value.url = "https://auth.example.com/authenticate";
    expect(validateAccessDraft(draft)).toBeNull();
    expect(draftValue(draft)).not.toHaveProperty("tls");
    const authorization = accessDraft({ ...endpoint, cache_ttl_seconds: 0 });
    expect(validateAccessDraft(authorization)).toBeNull();
    if ("cache_ttl_seconds" in authorization.value)
      authorization.value.cache_ttl_seconds = 301;
    expect(validateAccessDraft(authorization)).toContain("缓存时间");
  });
  it("resets secrets when changing service-auth kinds and does not mutate original config", () => {
    expect(changeServiceAuth("bearer")).toEqual({ type: "bearer", token: "" });
    expect(changeServiceAuth("basic")).toEqual({
      type: "basic",
      username: "",
      password: "",
    });
    const draft = accessDraft(endpoint);
    draft.value.headers["X-Test"] = "changed";
    expect(endpoint.headers).toEqual({});
  });
  it("never labels a fresh enabled backend healthy or classifies denials as service errors", () => {
    expect(stageHealth(stage)).toBe("暂无调用结果");
    expect(stageHealth({ ...stage, enabled: false })).toBe("未启用");
    expect(
      stageHealth({ ...stage, requests_total: 1, last_result: "deny" }),
    ).toBe("最近请求被拒绝");
    expect(
      stageHealth({ ...stage, requests_total: 1, last_result: "unavailable" }),
    ).toBe("服务异常");
  });
});

describe("PostgreSQL access-control configuration", () => {
  it("accepts configurable SQL/aliases and validates pool and secret limits", () => {
    const draft = accessDraft({
      ...endpoint,
      type: "postgresql",
      postgres: postgresDefaults(false),
    });
    expect(validateAccessDraft(draft, true)).toBeNull();
    const p = draft.value.postgres!;
    p.query =
      "SELECT existing_password AS password_hash FROM business_users WHERE account=$1";
    p.password = "[redacted]";
    expect(validateAccessDraft(draft, true)).toBeNull();
    expect(draftValue(draft).postgres?.query).toContain("business_users");
    p.max_connections = 33;
    expect(validateAccessDraft(draft)).toContain("连接池");
    p.max_connections = 8;
    p.query = " ";
    expect(validateAccessDraft(draft)).toContain("SQL");
  });
});

describe("Redis access-control configuration", () => {
  it("accepts the fixed key prefix and rejects malformed connection settings", () => {
    const draft = accessDraft({
      ...endpoint,
      type: "redis",
      redis: redisDefaults(),
    });
    expect(validateAccessDraft(draft, true)).toBeNull();
    const redis = draft.value.redis!;
    redis.password = "[redacted]";
    redis.username = "";
    expect(validateAccessDraft(draft, true)).toBeNull();
    redis.key_prefix = "tenant*";
    expect(validateAccessDraft(draft)).toContain("键前缀");
    redis.key_prefix = "tenant:";
    redis.max_connections = 0;
    expect(validateAccessDraft(draft)).toContain("连接池");
  });
});
