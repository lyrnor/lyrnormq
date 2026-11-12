import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { api, ApiError, errorMessage } from "./api";
import { Empty } from "./components";
import { Badge } from "./components/ui/badge";
import { Alert, AlertDescription } from "./components/ui/alert";
import { Button } from "./components/ui/button";
import { Card } from "./components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./components/ui/collapsible";
import { Field } from "./components/ui/field";
import { Switch } from "./components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./components/ui/tabs";
import { AccessControlFields } from "./AccessControlFields";
import { PostgresConnectionTest } from "./PostgresConnectionTest";
import { RedisConnectionTest } from "./RedisConnectionTest";
import { AccessControlTest } from "./AccessControlTest";
import {
  ACCESS_PATH,
  accessDraft,
  accessError,
  backendLabel,
  draftValue,
  stageHealth,
  validateAccessDraft,
  type AccessConfig,
  type AccessDraft,
  type AccessStage,
  type AccessStatus,
  type CacheStatus,
} from "./access-control-model";
import type { Configuration } from "./types";

const stageName = { authentication: "认证", authorization: "授权" };
function Notice({ children }: { children: React.ReactNode }) {
  return (
    <Alert variant="destructive">
      <AlertDescription>{children}</AlertDescription>
    </Alert>
  );
}

export function AccessControlPage({
  token,
  expired,
  refresh = 0,
  configuration,
}: {
  token: string;
  expired: () => void;
  refresh?: number;
  configuration?: Configuration | null;
}) {
  const [config, setConfig] = useState<AccessConfig | null>(null);
  const [drafts, setDrafts] = useState<Record<AccessStage, AccessDraft> | null>(
    null,
  );
  const [status, setStatus] = useState<AccessStatus | null>(null);
  const [stage, setStage] = useState<AccessStage>("authentication");
  const [reload, setReload] = useState(0);
  const [loading, setLoading] = useState(true),
    [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null),
    [statusError, setStatusError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null),
    [testing, setTesting] = useState<AccessStage | null>(null);
  const requests = useRef({
    load: null as AbortController | null,
    poll: null as AbortController | null,
    write: null as AbortController | null,
  });
  const writing = useRef(false);
  const onError = useCallback(
    (cause: unknown) => {
      if (cause instanceof ApiError && cause.status === 401) expired();
      else
        setError(
          cause instanceof ApiError
            ? (accessError(cause.code) ?? errorMessage(cause))
            : errorMessage(cause),
        );
    },
    [expired],
  );
  useEffect(() => {
    const controller = new AbortController();
    requests.current.load = controller;
    setLoading(true);
    setError(null);
    void api<AccessConfig>(`${ACCESS_PATH}/config`, token, controller.signal)
      .then((value) => {
        if (controller.signal.aborted) return;
        setConfig(value);
        setDrafts({
          authentication: accessDraft(value.authentication),
          authorization: accessDraft(value.authorization),
        });
      })
      .catch((cause) => {
        if (!controller.signal.aborted) onError(cause);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => {
      controller.abort();
      requests.current.write?.abort();
    };
  }, [token, onError, reload]);
  useEffect(() => {
    const controller = new AbortController();
    requests.current.poll = controller;
    let active = false;
    async function poll() {
      if (active || document.hidden) return;
      active = true;
      try {
        const value = await api<AccessStatus>(
          `${ACCESS_PATH}/status`,
          token,
          controller.signal,
        );
        if (!controller.signal.aborted) {
          setStatus(value);
          setStatusError(null);
        }
      } catch (cause) {
        if (!controller.signal.aborted) {
          if (cause instanceof ApiError && cause.status === 401) expired();
          else setStatusError("暂时无法更新调用状态，配置仍可编辑。");
        }
      } finally {
        active = false;
      }
    }
    void poll();
    const timer = window.setInterval(() => void poll(), 5000);
    const visible = () => {
      if (!document.hidden) void poll();
    };
    document.addEventListener("visibilitychange", visible);
    return () => {
      controller.abort();
      clearInterval(timer);
      document.removeEventListener("visibilitychange", visible);
    };
  }, [token, expired, refresh]);
  async function save(e: FormEvent) {
    e.preventDefault();
    if (!config || !drafts || writing.current) return;
    const invalid = validateAccessDraft(drafts[stage]);
    if (invalid) {
      setError(invalid);
      return;
    }
    const next = {
      ...config,
      [stage]: draftValue(drafts[stage]),
    } as AccessConfig;
    if (
      next.authorization.enabled &&
      !next.authentication.enabled &&
      configuration?.instance.auth_policy === false
    ) {
      setError(
        stage === "authentication"
          ? "请先停用 授权，再停用 认证，或先配置本地账号。"
          : "请先保存并启用 认证，或配置本地账号后再启用 授权。",
      );
      return;
    }
    writing.current = true;
    setBusy(true);
    setError(null);
    setNotice(null);
    const controller = new AbortController();
    requests.current.write = controller;
    try {
      const saved = await api<AccessConfig>(
        `${ACCESS_PATH}/config`,
        token,
        controller.signal,
        { method: "PUT", body: next },
      );
      if (!controller.signal.aborted) {
        setConfig(saved);
        setStatus((old) =>
          old
            ? {
                ...old,
                [stage]: { ...old[stage], enabled: saved[stage].enabled },
              }
            : old,
        );
        setDrafts((old) =>
          old ? { ...old, [stage]: accessDraft(saved[stage]) } : old,
        );
        setNotice(`${stageName[stage]}配置已保存。`);
      }
    } catch (cause) {
      if (!controller.signal.aborted) onError(cause);
    } finally {
      writing.current = false;
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  async function clearCache() {
    if (writing.current) return;
    writing.current = true;
    setBusy(true);
    setError(null);
    setNotice(null);
    const controller = new AbortController();
    requests.current.write = controller;
    try {
      const cache = await api<CacheStatus>(
        `${ACCESS_PATH}/cache/clear`,
        token,
        controller.signal,
        { method: "POST", body: {} },
      );
      if (!controller.signal.aborted) {
        setStatus((old) => (old ? { ...old, cache } : old));
        setNotice("授权缓存已清除。");
      }
    } catch (cause) {
      if (!controller.signal.aborted) onError(cause);
    } finally {
      writing.current = false;
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  if (loading && !drafts)
    return (
      <Card className="p-6">
        <Empty>正在读取认证与授权配置…</Empty>
      </Card>
    );
  if (!config || !drafts)
    return (
      <div className="grid gap-3">
        <Notice>{error ?? "暂时无法读取认证与授权配置。"}</Notice>
        <Button
          variant="outline"
          className="w-fit"
          onClick={() => setReload((value) => value + 1)}
        >
          重新加载
        </Button>
      </div>
    );
  const draft = drafts[stage],
    current = status?.[stage],
    label = stageName[stage];
  const update = (value: AccessDraft) =>
    setDrafts((old) => (old ? { ...old, [stage]: value } : old));
  const localAccounts = configuration?.instance.auth_policy;
  const fallback =
    !config.authentication.enabled && !config.authorization.enabled
      ? "认证与授权均未启用，当前使用原有认证与授权策略。"
      : !config.authorization.enabled &&
          localAccounts === false &&
          config.authentication.enabled
        ? "认证通过的设备当前可以发布和订阅所有主题。启用 授权可设置访问限制。"
        : !config.authorization.enabled
          ? localAccounts === true
            ? "授权未启用，当前使用本地授权策略。"
            : "授权未启用，具体权限由当前认证策略决定。"
          : null;
  return (
    <div className="grid gap-5">
      {fallback && (
        <Alert>
          <AlertDescription>{fallback}</AlertDescription>
        </Alert>
      )}
      {error && <Notice>{error}</Notice>}
      {notice && (
        <Alert role="status">
          <AlertDescription>{notice}</AlertDescription>
        </Alert>
      )}
      <Tabs
        value={stage}
        onValueChange={(value) => {
          setStage(value as AccessStage);
          setError(null);
          setNotice(null);
        }}
      >
        <TabsList aria-label="访问控制类型">
          <TabsTrigger disabled={busy} value="authentication">
            认证
          </TabsTrigger>
          <TabsTrigger disabled={busy} value="authorization">
            授权
          </TabsTrigger>
        </TabsList>
        <TabsContent value={stage} className="grid gap-5">
          <Card className="gap-4 p-5 shadow-none">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <h2 className="text-base font-semibold">
                {backendLabel(draft.value.type)} {label}
              </h2>
              <Badge
                variant={
                  current?.last_result === "unavailable" && current.enabled
                    ? "destructive"
                    : "secondary"
                }
              >
                {stageHealth(current)}
              </Badge>
            </div>
            <p className="text-xs leading-relaxed text-muted-foreground">
              {draft.value.type === "postgresql"
                ? "使用配置的 SQL 查询业务数据，数据库异常时拒绝本次新操作。"
                : draft.value.type === "redis"
                  ? stage === "authentication"
                    ? "按用户名读取 password_hash，并在 Broker 内校验 Argon2id。无记录或密码不匹配则拒绝本次连接。"
                    : "按用户名读取 publish 或 subscribe 规则。优先级高者优先，同级拒绝优先；无匹配则拒绝。"
                  : stage === "authentication"
                  ? "校验设备用户名和密码，通过返回 204，拒绝返回 401。"
                  : "校验设备对指定 Topic 的操作权限，允许返回 204，拒绝返回 403。"}
            </p>
            <div className="grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
              {[
                ["已完成请求", current?.requests_total],
                ["通过", current?.allowed_total],
                ["拒绝", current?.denied_total],
                ["服务异常", current?.errors_total],
              ].map(([name, value]) => (
                <div key={name} className="rounded-md bg-muted/60 p-3">
                  <div className="text-xs text-muted-foreground">{name}</div>
                  <div className="mt-1 font-semibold">
                    {typeof value === "number" ? value.toLocaleString() : "—"}
                  </div>
                </div>
              ))}
            </div>
            {current?.pool_connections !== undefined && (
              <p className="text-xs text-muted-foreground">
                {draft.value.type === "redis" ? "Redis" : "数据库"}连接：
                {current.pool_connections}，空闲：
                {current.pool_idle_connections ?? 0}
              </p>
            )}
            {statusError && (
              <p className="text-xs text-muted-foreground">{statusError}</p>
            )}
          </Card>
          <Card className="p-5 shadow-none">
            <form
              onSubmit={(e) => void save(e)}
              className="grid max-w-3xl gap-5"
            >
              <div className="flex items-center justify-between gap-4 border-b pb-4">
                <h3 className="font-semibold">{label}服务</h3>
                <Field label={`启用${label}`}>
                  <Switch
                    checked={draft.value.enabled}
                    disabled={busy}
                    onCheckedChange={(enabled) =>
                      update({ ...draft, value: { ...draft.value, enabled } })
                    }
                  />
                </Field>
              </div>
              <AccessControlFields
                key={`fields-${stage}`}
                draft={draft}
                postgresAvailable={status?.postgresql_available !== false}
                redisAvailable={status?.redis_available !== false}
                onChange={update}
                busy={busy}
              />
              {draft.value.type === "postgresql" && (
                <PostgresConnectionTest
                  key={`connection-test-${stage}`}
                  draft={draft}
                  stage={stage}
                  token={token}
                  expired={expired}
                  disabled={busy}
                />
              )}
              {draft.value.type === "redis" && (
                <RedisConnectionTest
                  key={`redis-connection-test-${stage}`}
                  draft={draft}
                  stage={stage}
                  token={token}
                  expired={expired}
                  disabled={busy}
                />
              )}
              {draft.value.type === "http" && (
                <Collapsible className="rounded-lg border">
                  <CollapsibleTrigger className="w-full px-4 py-3 text-left text-sm font-medium">
                    查看接口示例
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="border-t p-4 text-xs leading-relaxed text-muted-foreground">
                      <p>由 Broker 调用你的服务，请求内容自动生成。</p>
                      <pre className="mt-3 overflow-auto rounded-md bg-muted p-3 text-foreground">
                        {JSON.stringify(
                          stage === "authentication"
                            ? {
                                client_id: "device-001",
                                username: "device-001",
                                password: "******",
                              }
                            : {
                                client_id: "device-001",
                                username: "device-001",
                                action: "publish",
                                topic: "devices/device-001/telemetry",
                              },
                          null,
                          2,
                        )}
                      </pre>
                      <p className="mt-3">
                        {stage === "authentication"
                          ? "返回 204 表示认证通过，401 表示认证失败。"
                          : "返回 204 表示允许，403 表示拒绝当前操作。"}
                        不需要响应内容。
                      </p>
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              )}
              {stage === "authorization" && (
                <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border p-4">
                  <div>
                    <p className="text-sm font-medium">授权缓存</p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      清除后，下次操作重新查询权限。
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    disabled={busy}
                    onClick={() => void clearCache()}
                  >
                    清除缓存
                  </Button>
                </div>
              )}
              <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
                <Button
                  type="button"
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    const invalid = validateAccessDraft(draft, true);
                    if (invalid) {
                      setError(invalid);
                      return;
                    }
                    setError(null);
                    setTesting(stage);
                  }}
                >
                  测试{label}
                </Button>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    disabled={busy}
                    onClick={() => {
                      update(accessDraft(config[stage]));
                      setError(null);
                      setNotice(null);
                    }}
                  >
                    取消
                  </Button>
                  <Button type="submit" disabled={busy}>
                    {busy ? "正在处理…" : "保存"}
                  </Button>
                </div>
              </div>
            </form>
          </Card>
        </TabsContent>
      </Tabs>
      {testing && (
        <AccessControlTest
          key={testing}
          stage={testing}
          draft={drafts[testing]}
          token={token}
          expired={expired}
          onClose={() => setTesting(null)}
        />
      )}
    </div>
  );
}
