export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public code?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}
export async function api<T>(
  path: string,
  token: string,
  signal?: AbortSignal,
  options?: { method: "POST" | "PUT"; body: unknown },
): Promise<T> {
  if (
    !path.startsWith("/api/v1/") ||
    path.includes("://") ||
    /[\r\n]/.test(token)
  )
    throw new ApiError(400, "无效的管理请求");
  const body = options ? JSON.stringify(options.body) : undefined;
  if (body && new TextEncoder().encode(body).length > 65536)
    throw new ApiError(413, "请求内容最多 64 KiB");
  if (
    options &&
    !(
      (path === "/api/v1/rules/config" && options.method === "PUT") ||
      (path === "/api/v1/rules/test" && options.method === "POST") ||
      (path === "/api/v1/access-control/config" && options.method === "PUT") ||
      ([
        "/api/v1/access-control/test/authentication",
        "/api/v1/access-control/test/authorization",
        "/api/v1/access-control/cache/clear",
      ].includes(path) &&
        options.method === "POST")
    )
  )
    throw new ApiError(400, "该管理接口不支持写入");
  const response = await fetch(path, {
    ...(options ? { method: options.method, body } : {}),
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(options ? { "Content-Type": "application/json" } : {}),
    },
    signal: signal
      ? AbortSignal.any([signal, AbortSignal.timeout(10000)])
      : AbortSignal.timeout(10000),
    cache: "no-store",
    credentials: "omit",
    redirect: "error",
  });
  if (!response.ok) {
    let message =
      response.status === 401
        ? "管理令牌无效或已失效"
        : response.status === 503
          ? "Broker 尚未就绪或已经停止"
          : response.status === 429
            ? "管理查询繁忙，请稍后重试"
            : `请求失败（${response.status}）`;
    let code: string | undefined;
    try {
      const data = await response.json();
      if (
        typeof data?.error?.code === "string" &&
        /^[a-z_]{1,64}$/.test(data.error.code)
      )
        code = data.error.code;
    } catch {
      /* Keep the public status message; never expose response contents. */
    }
    if (code === "rules_disabled") message = "规则引擎尚未启用，请联系管理员";
    else if (response.status === 400 && path.startsWith("/api/v1/rules/"))
      message = "规则 SQL 或配置无效，请检查语法、参数范围和引用关系";
    else if (
      response.status === 400 &&
      path.startsWith("/api/v1/access-control/")
    )
      message = "认证或授权配置无效，请检查地址、鉴权和填写内容";
    throw new ApiError(response.status, message, code);
  }
  if (!response.headers.get("content-type")?.includes("application/json"))
    throw new ApiError(502, "服务响应异常，请稍后重试");
  try {
    return (await response.json()) as T;
  } catch {
    throw new ApiError(502, "服务响应异常，请稍后重试");
  }
}
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof DOMException && error.name === "TimeoutError")
    return "请求超时，请稍后重试";
  return "无法连接服务，请检查网络连接";
}
