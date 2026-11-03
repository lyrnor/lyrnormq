import React from "react";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccessControlPage } from "../src/AccessControlPage";
import { api, ApiError } from "../src/api";
import {
  ACCESS_PATH,
  type AccessConfig,
  type AccessStatus,
} from "../src/access-control-model";
vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));
const request = vi.mocked(api),
  expired = vi.fn();
const config: AccessConfig = {
  schema_version: 1,
  authentication: {
    type: "http",
    enabled: false,
    name: "HTTP 认证",
    url: "https://auth.example.com/authenticate",
    service_auth: { type: "none" },
    headers: {},
    timeout_ms: 1000,
    max_concurrency: 128,
    allow_http: false,
  },
  authorization: {
    type: "http",
    enabled: false,
    name: "HTTP 授权",
    url: "https://auth.example.com/acl",
    service_auth: { type: "none" },
    headers: {},
    timeout_ms: 1000,
    max_concurrency: 128,
    allow_http: false,
    cache_ttl_seconds: 30,
  },
};
const baseStage = {
  enabled: false,
  requests_total: 0,
  allowed_total: 0,
  denied_total: 0,
  errors_total: 0,
  last_result: null,
  last_http_status: null,
  last_latency_ms: null,
};
const status: AccessStatus = {
  authentication: { ...baseStage },
  authorization: { ...baseStage },
  cache: { entries: 2, bytes: 300, hits: 4, misses: 2, inflight: 0 },
};
function server(initial = config, stats = status) {
  let saved = structuredClone(initial);
  request.mockImplementation(async (path, _token, _signal, options) => {
    if (path === `${ACCESS_PATH}/config` && options?.method === "PUT") {
      saved = structuredClone(options.body as AccessConfig);
      return saved as never;
    }
    if (path === `${ACCESS_PATH}/config`)
      return structuredClone(saved) as never;
    if (path.endsWith("/status")) return structuredClone(stats) as never;
    if (path.endsWith("/cache/clear"))
      return { ...status.cache, entries: 0, bytes: 0 } as never;
    if (path.includes("/test/"))
      return {
        result: "deny",
        http_status: path.endsWith("authentication") ? 401 : 403,
        elapsed_ms: 12.4,
        error_code: null,
      } as never;
    throw new Error("unexpected API");
  });
  return () => saved;
}
async function ready() {
  await screen.findByLabelText("请求地址");
}
async function tab(name: string) {
  fireEvent.mouseDown(screen.getByRole("tab", { name }), {
    button: 0,
    ctrlKey: false,
  });
  await ready();
}
async function select(name: string, option: string) {
  fireEvent.keyDown(screen.getByRole("button", { name }), { key: "ArrowDown" });
  fireEvent.click(await screen.findByRole("menuitemradio", { name: option }));
  await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
}
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
  request.mockReset();
  expired.mockReset();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});
describe("HTTP authentication and authorization dashboard", () => {
  it("uses basic fields and collapsed tuning, with no editable body/internal storage controls", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    expect((screen.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "HTTP 认证",
    );
    expect(screen.getByRole("switch", { name: "启用认证" })).toBeTruthy();
    expect(screen.queryByLabelText("请求超时（ms）")).toBeNull();
    expect(screen.queryByLabelText("ID")).toBeNull();
    expect(screen.queryByText(/SQLite|outbox|队列容量|SDK/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    expect(
      (screen.getByLabelText("请求超时（ms）") as HTMLInputElement).value,
    ).toBe("1000");
    expect((screen.getByLabelText("最大并发") as HTMLInputElement).value).toBe(
      "128",
    );
    expect(screen.queryByLabelText("决策缓存时间（秒）")).toBeNull();
    await tab("授权");
    fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
    expect(
      (screen.getByLabelText("决策缓存时间（秒）") as HTMLInputElement).value,
    ).toBe("30");
    expect(screen.getByRole("button", { name: "清除缓存" })).toBeTruthy();
  });
  it("saves only the active form and preserves the other tab's unsaved draft", async () => {
    const saved = server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Uncommitted auth" },
    });
    await tab("授权");
    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "Saved ACL" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await screen.findByText("授权配置已保存。");
    expect(saved().authentication.name).toBe("HTTP 认证");
    expect(saved().authorization.name).toBe("Saved ACL");
    await tab("认证");
    expect((screen.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "Uncommitted auth",
    );
    fireEvent.click(screen.getByRole("button", { name: "取消" }));
    expect((screen.getByLabelText("名称") as HTMLInputElement).value).toBe(
      "HTTP 认证",
    );
  });
  it("supports service credentials with redacted preservation and never browser persistence", async () => {
    const initial = structuredClone(config);
    initial.authentication.service_auth = {
      type: "bearer",
      token: "[redacted]",
    };
    const saved = server(initial);
    const persist = vi.spyOn(Storage.prototype, "setItem");
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    expect((screen.getByLabelText("服务 Token") as HTMLInputElement).type).toBe(
      "password",
    );
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await screen.findByText("认证配置已保存。");
    expect(saved().authentication.service_auth).toEqual({
      type: "bearer",
      token: "[redacted]",
    });
    await select("服务鉴权", "Basic Auth");
    expect(screen.queryByLabelText("服务 Token")).toBeNull();
    fireEvent.change(screen.getByLabelText("服务用户名"), {
      target: { value: "service-user" },
    });
    fireEvent.change(screen.getByLabelText("服务密码"), {
      target: { value: "service-secret" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() =>
      expect(saved().authentication.service_auth.type).toBe("basic"),
    );
    expect(persist).not.toHaveBeenCalled();
  });
  it("probes the unsaved draft, treats external401 as denial and wipes the device password", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    fireEvent.change(screen.getByLabelText("请求地址"), {
      target: { value: "https://draft.example.com/auth" },
    });
    fireEvent.click(screen.getByRole("button", { name: "测试认证" }));
    const drawer = screen.getByRole("dialog");
    fireEvent.change(within(drawer).getByLabelText("Client ID"), {
      target: { value: "device-client" },
    });
    fireEvent.change(within(drawer).getByLabelText("设备 Username"), {
      target: { value: "device-name" },
    });
    fireEvent.change(within(drawer).getByLabelText("设备 Password"), {
      target: { value: "device-secret" },
    });
    fireEvent.click(within(drawer).getByRole("button", { name: "开始测试" }));
    await within(drawer).findByText("认证失败");
    expect(expired).not.toHaveBeenCalled();
    const body = request.mock.calls.find(([path]) =>
      path.endsWith("/test/authentication"),
    )?.[3]?.body as Record<string, unknown>;
    expect(body).toMatchObject({
      client_id: "device-client",
      username: "device-name",
      password: "device-secret",
      config: { url: "https://draft.example.com/auth", enabled: false },
    });
    expect(
      (within(drawer).getByLabelText("设备 Password") as HTMLInputElement)
        .value,
    ).toBe("");
    expect(
      request.mock.calls.filter(([, , , options]) => options?.method === "PUT"),
    ).toHaveLength(0);
    fireEvent.click(within(drawer).getByRole("button", { name: "关闭详情" }));
    fireEvent.click(screen.getByRole("button", { name: "测试认证" }));
    expect(
      (screen.getByLabelText("设备 Password") as HTMLInputElement).value,
    ).toBe("");
  });
  it("sends concrete ACL inputs and clears cache through the protected management API", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    await tab("授权");
    fireEvent.click(screen.getByRole("button", { name: "测试授权" }));
    const drawer = screen.getByRole("dialog");
    for (const [label, value] of [
      ["Client ID", "client"],
      ["设备 Username", "device"],
      ["Topic", "devices/a"],
    ])
      fireEvent.change(within(drawer).getByLabelText(label), {
        target: { value },
      });
    fireEvent.click(within(drawer).getByRole("button", { name: "开始测试" }));
    await within(drawer).findByText("拒绝当前操作");
    expect(
      request.mock.calls.find(([path]) =>
        path.endsWith("/test/authorization"),
      )?.[3]?.body,
    ).toMatchObject({
      client_id: "client",
      username: "device",
      action: "publish",
      topic: "devices/a",
      config: { cache_ttl_seconds: 30 },
    });
    fireEvent.click(within(drawer).getByRole("button", { name: "关闭详情" }));
    fireEvent.click(screen.getByRole("button", { name: "清除缓存" }));
    await screen.findByText("授权缓存已清除。");
    expect(
      request.mock.calls.find(([path]) => path.endsWith("/cache/clear"))?.[3],
    ).toEqual({ method: "POST", body: {} });
  });
  it("distinguishes no live calls and business rejection from backend failure", async () => {
    const stats = structuredClone(status);
    stats.authentication.enabled = true;
    server(config, stats);
    const { unmount } = render(
      <AccessControlPage token="token" expired={expired} />,
    );
    await ready();
    expect(screen.getByText("暂无调用结果")).toBeTruthy();
    unmount();
    stats.authentication = {
      ...stats.authentication,
      requests_total: 1,
      denied_total: 1,
      last_result: "deny",
      last_http_status: 401,
      last_latency_ms: 2,
    };
    server(config, stats);
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    expect(screen.getByText("最近请求被拒绝")).toBeTruthy();
    expect(screen.queryByText("服务异常", { selector: "span" })).toBeNull();
  });
  it("retains editability on status failures and expires only actual admin401", async () => {
    server();
    request.mockImplementation(async (path) => {
      if (path.endsWith("/status"))
        throw new ApiError(503, "secret backend diagnostic");
      return config as never;
    });
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    await screen.findByText("暂时无法更新调用状态，配置仍可编辑。");
    expect(screen.queryByText(/secret backend/)).toBeNull();
    cleanup();
    request.mockRejectedValue(new ApiError(401, "invalid token"));
    render(<AccessControlPage token="token" expired={expired} />);
    await waitFor(() => expect(expired).toHaveBeenCalled());
  });
  it("updates enabled state after saving without claiming uncalled services healthy", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    fireEvent.click(screen.getByRole("switch", { name: "启用认证" }));
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await screen.findByText("认证配置已保存。");
    expect(screen.getByText("暂无调用结果")).toBeTruthy();
    expect(screen.queryByText("未启用", { selector: "span" })).toBeNull();
  });
  it("can retry a failed configuration load without requiring a page reload", async () => {
    request.mockRejectedValue(new ApiError(503, "not ready"));
    render(<AccessControlPage token="token" expired={expired} />);
    await screen.findByRole("button", { name: "重新加载" });
    server();
    fireEvent.click(screen.getByRole("button", { name: "重新加载" }));
    await ready();
    expect(screen.getByLabelText("名称")).toBeTruthy();
  });
  it("omits certificate controls and TLS configuration from both saved forms and probes", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    for (const name of ["认证", "授权"]) {
      await tab(name);
      fireEvent.click(screen.getByRole("button", { name: "高级设置" }));
      expect(screen.queryByText("HTTPS 证书（可选）")).toBeNull();
      expect(screen.queryByLabelText("CA 证书")).toBeNull();
      expect(screen.queryByLabelText("客户端证书")).toBeNull();
      expect(screen.queryByLabelText("客户端私钥")).toBeNull();
      expect(document.querySelector('input[type="file"]')).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "保存" }));
      await screen.findByText(`${name}配置已保存。`);
      const saved = request.mock.calls
        .filter(([, , , options]) => options?.method === "PUT")
        .at(-1)?.[3]?.body as AccessConfig;
      expect(saved.authentication).not.toHaveProperty("tls");
      expect(saved.authorization).not.toHaveProperty("tls");
      fireEvent.click(screen.getByRole("button", { name: `测试${name}` }));
      const drawer = screen.getByRole("dialog");
      fireEvent.change(within(drawer).getByLabelText("Client ID"), {
        target: { value: "client" },
      });
      fireEvent.change(within(drawer).getByLabelText("设备 Username"), {
        target: { value: "device" },
      });
      if (name === "授权")
        fireEvent.change(within(drawer).getByLabelText("Topic"), {
          target: { value: "devices/one" },
        });
      fireEvent.click(within(drawer).getByRole("button", { name: "开始测试" }));
      await waitFor(() =>
        expect(
          request.mock.calls.some(([path]) =>
            path.endsWith(
              `/test/${name === "认证" ? "authentication" : "authorization"}`,
            ),
          ),
        ).toBe(true),
      );
      const probe = request.mock.calls
        .filter(([path]) => path.includes("/test/"))
        .at(-1)?.[3]?.body as { config: unknown };
      expect(probe.config).not.toHaveProperty("tls");
      fireEvent.click(within(drawer).getByRole("button", { name: "关闭详情" }));
    }
  });
  it("cancels an in-flight draft test when the drawer closes", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    let resolve: (result: unknown) => void = () => {};
    let signal: AbortSignal | undefined;
    request.mockImplementation(async (path, _token, suppliedSignal) => {
      if (path.includes("/test/")) {
        signal = suppliedSignal;
        return (await new Promise<unknown>((r) => {
          resolve = r;
        })) as never;
      }
      return status as never;
    });
    fireEvent.click(screen.getByRole("button", { name: "测试认证" }));
    const drawer = screen.getByRole("dialog");
    fireEvent.change(within(drawer).getByLabelText("Client ID"), {
      target: { value: "client" },
    });
    fireEvent.change(within(drawer).getByLabelText("设备 Username"), {
      target: { value: "device" },
    });
    fireEvent.click(within(drawer).getByRole("button", { name: "开始测试" }));
    fireEvent.click(within(drawer).getByRole("button", { name: "关闭详情" }));
    expect(signal?.aborted).toBe(true);
    await act(async () => {
      resolve({
        result: "allow",
        http_status: 204,
        elapsed_ms: 2,
        error_code: null,
      });
    });
    expect(screen.queryByText("认证通过")).toBeNull();
  });
});

describe("PostgreSQL authentication dashboard", () => {
  it("keeps one live backend selector when switching backends and stages repeatedly", async () => {
    server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    for (let round = 0; round < 3; round++) {
      for (const name of ["认证", "授权"]) {
        fireEvent.mouseDown(screen.getByRole("tab", { name }), {
          button: 0,
          ctrlKey: false,
        });
        await select("后端类型", "PostgreSQL");
        // Reconcile keyed siblings while the connection test is mounted.
        for (const tabName of [name === "认证" ? "授权" : "认证", name]) {
          fireEvent.mouseDown(screen.getByRole("tab", { name: tabName }), {
            button: 0,
            ctrlKey: false,
          });
        }
        expect(
          screen.getAllByRole("button", { name: "后端类型" }),
        ).toHaveLength(1);
        expect(screen.getAllByLabelText("数据库主机")).toHaveLength(1);
        expect(
          screen.getByRole("heading", { name: `PostgreSQL ${name}` }),
        ).toBeTruthy();
        await select("后端类型", "HTTP");
        expect(
          screen.getAllByRole("button", { name: "后端类型" }),
        ).toHaveLength(1);
        await waitFor(() =>
          expect(screen.queryByLabelText("数据库主机")).toBeNull(),
        );
        expect(screen.getAllByLabelText("请求地址")).toHaveLength(1);
        expect(
          screen.getByRole("heading", { name: `HTTP ${name}` }),
        ).toBeTruthy();
      }
    }
  });
  it("switches backends, saves custom SQL and offers database connection testing", async () => {
    const saved = server();
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    await select("后端类型", "PostgreSQL");
    expect(screen.queryByLabelText("请求地址")).toBeNull();
    expect(screen.getByLabelText("数据库主机")).toBeTruthy();
    expect(screen.getByLabelText("数据库 TLS")).toBeTruthy();
    const sql =
      "SELECT credential AS password_hash FROM accounts WHERE login = $1";
    fireEvent.change(screen.getByLabelText("查询 SQL"), {
      target: { value: sql },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await screen.findByText("认证配置已保存。");
    expect(saved().authentication.type).toBe("postgresql");
    expect(saved().authentication.postgres?.query).toBe(sql);
    fireEvent.click(screen.getByRole("button", { name: "测试数据库连接" }));
    await screen.findByText(/数据库连接失败/);
    expect(
      request.mock.calls.some(
        ([path, , , options]) =>
          path.endsWith("/test/connection") &&
          (options?.body as { stage?: string })?.stage === "authentication",
      ),
    ).toBe(true);
    expect(localStorage.length).toBe(0);
  });
});

describe("compiled access backend availability", () => {
  it("hides PostgreSQL when the broker was built without it", async () => {
    server(config, { ...status, postgresql_available: false });
    render(<AccessControlPage token="token" expired={expired} />);
    await ready();
    fireEvent.keyDown(screen.getByRole("button", { name: "后端类型" }), {
      key: "ArrowDown",
    });
    await screen.findByRole("menuitemradio", { name: "HTTP" });
    expect(
      screen.queryByRole("menuitemradio", { name: "PostgreSQL" }),
    ).toBeNull();
  });
});
