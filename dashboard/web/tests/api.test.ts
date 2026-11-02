import { afterEach, describe, it, expect, vi } from "vitest";
import { api, ApiError, errorMessage } from "../src/api";
afterEach(() => vi.unstubAllGlobals());
describe("same-origin read-only API client", () => {
  it("sends bearer only in headers, omits cookies/cache and refuses redirects", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response('{"ready":true}', {
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);
    expect(await api("/api/v1/overview", "secret")).toEqual({ ready: true });
    const [url, options] = fetch.mock.calls[0];
    expect(url).toBe("/api/v1/overview");
    expect(options.headers.Authorization).toBe("Bearer secret");
    expect(options.credentials).toBe("omit");
    expect(options.cache).toBe("no-store");
    expect(options.redirect).toBe("error");
    expect(options.signal).toBeInstanceOf(AbortSignal);
  });
  it("preserves cancellation and bounded timeout", async () => {
    const controller = new AbortController();
    const fetch = vi.fn().mockImplementation((_url, options) => {
      expect(options.signal.aborted).toBe(true);
      return Promise.reject(new DOMException("aborted", "AbortError"));
    });
    vi.stubGlobal("fetch", fetch);
    controller.abort();
    await expect(
      api("/api/v1/overview", "secret", controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
  });
  it("rejects foreign URLs and newline bearer injection before fetch", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    await expect(
      api("https://evil.invalid/api/v1/overview", "x"),
    ).rejects.toBeInstanceOf(ApiError);
    await expect(
      api("/api/v1/overview", "x\r\nHeader:value"),
    ).rejects.toBeInstanceOf(ApiError);
    expect(fetch).not.toHaveBeenCalled();
  });
  it("distinguishes auth expiry, overload and readiness failures without exposing response internals", async () => {
    for (const status of [401, 429, 503]) {
      vi.stubGlobal(
        "fetch",
        vi.fn().mockResolvedValue(new Response("secret SQL path", { status })),
      );
      await expect(api("/api/v1/overview", "secret")).rejects.toMatchObject({
        status,
      });
      try {
        await api("/api/v1/overview", "secret");
      } catch (error) {
        expect(errorMessage(error)).not.toContain("secret SQL");
      }
    }
  });
  it("rejects HTML fallbacks and malformed JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response("<html>", { headers: { "Content-Type": "text/html" } }),
        ),
    );
    await expect(api("/api/v1/overview", "secret")).rejects.toMatchObject({
      status: 502,
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("{", {
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    await expect(api("/api/v1/overview", "secret")).rejects.toMatchObject({
      status: 502,
    });
  });
});

describe("bounded rule management writes", () => {
  it("sends authenticated JSON only to the supported rule write endpoints", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ rules: [], connectors: [], actions: [] }),
          { headers: { "Content-Type": "application/json" } },
        ),
      );
    vi.stubGlobal("fetch", fetch);
    await api("/api/v1/rules/config", "secret", undefined, {
      method: "PUT",
      body: { rules: [], connectors: [], actions: [] },
    });
    const options = fetch.mock.calls[0][1];
    expect(options.method).toBe("PUT");
    expect(options.headers["Content-Type"]).toBe("application/json");
    expect(options.headers.Authorization).toBe("Bearer secret");
    await expect(
      api("/api/v1/sessions", "secret", undefined, { method: "PUT", body: {} }),
    ).rejects.toMatchObject({ status: 400 });
    await expect(
      api("/api/v1/rules/config", "secret", undefined, {
        method: "POST",
        body: {},
      }),
    ).rejects.toMatchObject({ status: 400 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it("rejects requests above 64KiB before any network call", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    await expect(
      api("/api/v1/rules/test", "secret", undefined, {
        method: "POST",
        body: { payload: "中".repeat(22000) },
      }),
    ).rejects.toMatchObject({ status: 413 });
    expect(fetch).not.toHaveBeenCalled();
  });
  it("exposes only safe error codes and no server credential details", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: {
              code: "rules_disabled",
              message: "secret path and password",
            },
          }),
          { status: 503, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );
    await expect(api("/api/v1/rules/config", "secret")).rejects.toMatchObject({
      code: "rules_disabled",
      message: expect.stringContaining("请联系管理员"),
    });
    try {
      await api("/api/v1/rules/config", "secret");
    } catch (error) {
      expect(errorMessage(error)).not.toContain("password");
    }
  });
});

describe("bounded access-control management writes", () => {
  it("allows only exact supported methods and endpoints with JSON results", async () => {
    const fetch = vi.fn().mockImplementation(
      async () =>
        new Response('{"result":"deny","http_status":401}', {
          headers: { "Content-Type": "application/json" },
        }),
    );
    vi.stubGlobal("fetch", fetch);
    for (const [path, method] of [
      ["/api/v1/access-control/config", "PUT"],
      ["/api/v1/access-control/test/authentication", "POST"],
      ["/api/v1/access-control/test/authorization", "POST"],
      ["/api/v1/access-control/cache/clear", "POST"],
    ] as const) {
      const response = await api<{ result: string; http_status: number }>(
        path,
        "secret",
        undefined,
        { method, body: {} },
      );
      expect(response.http_status).toBe(401);
    }
    for (const [path, method] of [
      ["/api/v1/access-control/config", "POST"],
      ["/api/v1/access-control/test/authentication", "PUT"],
      ["/api/v1/access-control/test/authorization?unsafe=x", "POST"],
      ["/api/v1/access-control/cache/clear/extra", "POST"],
      ["/api/v1/access-control/status", "PUT"],
    ] as const)
      await expect(
        api(path, "secret", undefined, { method, body: {} }),
      ).rejects.toMatchObject({ status: 400 });
    expect(fetch).toHaveBeenCalledTimes(4);
  });
  it("keeps public validation errors friendly without exposing backend diagnostics", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            '{"error":{"code":"access_invalid_config","message":"secret SQL and password"}}',
            { status: 400, headers: { "Content-Type": "application/json" } },
          ),
        ),
    );
    try {
      await api("/api/v1/access-control/config", "secret", undefined, {
        method: "PUT",
        body: {},
      });
      throw new Error("expected rejection");
    } catch (error) {
      expect(errorMessage(error)).toContain("配置无效");
      expect(errorMessage(error)).not.toContain("secret");
    }
  });
});
