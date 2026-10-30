export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}
export async function api<T>(
  path: string,
  token: string,
  signal?: AbortSignal,
): Promise<T> {
  if (
    !path.startsWith("/api/v1/") ||
    path.includes("://") ||
    /[\r\n]/.test(token)
  )
    throw new ApiError(400, "无效的管理请求");
  const response = await fetch(path, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
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
            : `管理 API 请求失败（${response.status}）`;
    throw new ApiError(response.status, message);
  }
  if (!response.headers.get("content-type")?.includes("application/json"))
    throw new ApiError(502, "管理 API 返回了非 JSON 数据");
  try {
    return (await response.json()) as T;
  } catch {
    throw new ApiError(502, "管理 API 响应无法解析");
  }
}
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof DOMException && error.name === "TimeoutError")
    return "管理 API 请求超时";
  return "无法连接管理 API，请检查 Broker 和网络连接";
}
