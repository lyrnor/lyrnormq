import { Button } from "./components/ui/button";
import { Card } from "./components/ui/card";
import { Input } from "./components/ui/input";
import { NativeSelect } from "./components/ui/native-select";
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "./components/ui/table";
import React, {
  useCallback,
  useEffect,
  useReducer,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { api, ApiError, errorMessage } from "./api";
import { RulesPage } from "./RulesPage";
import {
  authReducer,
  boundedSearch,
  bytes,
  changeQuery,
  duration,
  initialQuery,
  protocolLabel,
  routeFromHash,
} from "./model";
import { useBroker } from "./useBroker";
import {
  Badge,
  Drawer,
  Empty,
  Heading,
  Icon,
  KeyValues,
  Paging,
  Panel,
  Traffic,
} from "./components";
import type { Configuration, Overview, Query, Route, Session } from "./types";
const labels: Record<Route, string> = {
  overview: "运行概览",
  connections: "连接与会话",
  topics: "主题与消息",
  security: "认证与授权",
  listeners: "连接入口",
  configuration: "实例配置",
  logs: "事件记录",
  rules: "规则引擎",
};
function Brand() {
  return (
    <a
      className="brand flex items-center gap-3 px-6 pt-7 pb-6 text-[27px] font-bold tracking-tight [&_small]:mt-1 [&_small]:block [&_small]:text-[8px] [&_small]:font-medium [&_small]:tracking-[.24em] [&_small]:text-muted-foreground"
      href="#overview"
      aria-label="rmqtt 首页"
    >
      <span className="relative grid h-10 w-9 shrink-0 place-items-center rounded-[10px_10px_10px_3px] bg-[#e9875d] text-[34px] leading-none font-bold text-white [&>span]:absolute [&>span]:right-2 [&>span]:bottom-2 [&>span]:size-1.5 [&>span]:rounded-full [&>span]:bg-white">
        r<span />
      </span>
      <span>
        rmqtt<small>BROKER CONSOLE</small>
      </span>
    </a>
  );
}
function Login({
  onLogin,
  error,
}: {
  onLogin: (token: string) => void;
  error: string | null;
}) {
  const [value, setValue] = useState(""),
    [busy, setBusy] = useState(false),
    [message, setMessage] = useState(error);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  async function submit(e: FormEvent) {
    e.preventDefault();
    if (busy) return;
    setBusy(true);
    setMessage(null);
    request.current = new AbortController();
    try {
      await api<Overview>(
        "/api/v1/overview",
        value.trim(),
        request.current.signal,
      );
      onLogin(value.trim());
      setValue("");
    } catch (cause) {
      if (!request.current.signal.aborted) setMessage(errorMessage(cause));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="grid min-h-dvh place-items-center bg-background p-5">
      <section className="w-full max-w-[440px] rounded-2xl border bg-card p-7 shadow-xl shadow-emerald-950/5 sm:p-9 [&_.brand]:px-0 [&_.brand]:pt-0 [&_.brand]:pb-9 [&_h1]:mb-3 [&_h1]:text-2xl [&_h1]:font-semibold [&>p]:text-xs [&>p]:leading-relaxed [&>p]:text-muted-foreground [&_form]:mt-7 [&_label]:mb-3 [&_label]:block [&_label]:text-xs [&_label]:font-semibold [&_input]:mb-4 [&_form>button]:w-full">
        <Brand />
        <div className="mb-2 text-[9px] font-semibold tracking-[.18em] text-muted-foreground">
          SINGLE NODE / LIVE CONSOLE
        </div>
        <h1>连接到 Broker</h1>
        <p>使用独立管理令牌，查看当前实例的实时状态。</p>
        <form onSubmit={(e) => void submit(e)}>
          <label htmlFor="token">管理令牌</label>
          <Input
            id="token"
            name="token"
            type="password"
            autoComplete="off"
            placeholder="输入管理令牌"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            minLength={32}
            maxLength={256}
            required
            disabled={busy}
          />
          {message && (
            <div
              className="mb-4 rounded-lg border border-orange-200 bg-orange-50 px-4 py-3 text-xs leading-relaxed break-words text-orange-800 [&_strong]:font-medium [&_span]:mt-1 [&_span]:block [&_span]:text-[11px]"
              role="alert"
            >
              {message}
            </div>
          )}
          <Button
            variant="default"
            className="h-8 gap-2 rounded-md px-3 text-xs font-medium"
            disabled={busy || value.trim().length < 32}
          >
            {busy ? "正在验证…" : "进入控制台"}
            <span aria-hidden="true"> →</span>
          </Button>
        </form>
        <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
          刷新页面后需重新登录。请使用管理员提供的访问令牌。
        </div>
        <small className="mt-7 block text-center text-[10px] tracking-wider text-muted-foreground">
          Rust · Tokio · rustls
        </small>
      </section>
    </div>
  );
}
function Console({
  token,
  logout,
  expired,
}: {
  token: string;
  logout: () => void;
  expired: () => void;
}) {
  const [route, setRoute] = useState<Route>(() => routeFromHash(location.hash)),
    [query, setQuery] = useState<Query>(initialQuery),
    [search, setSearch] = useState(""),
    [topicTab, setTopicTab] = useState("subscriptions"),
    [refresh, setRefresh] = useState(0),
    [menu, setMenu] = useState(false);
  const [detail, setDetail] = useState<Session | null>(null),
    [detailError, setDetailError] = useState<string | null>(null),
    [detailLoading, setDetailLoading] = useState(false),
    [detailId, setDetailId] = useState<string | null>(null);
  const detailRequest = useRef<AbortController | null>(null);
  const closeDetail = useCallback(() => {
    detailRequest.current?.abort();
    setDetail(null);
    setDetailId(null);
    setDetailError(null);
    setDetailLoading(false);
  }, []);
  useEffect(() => {
    const change = () => {
      setRoute(routeFromHash(location.hash));
      setQuery(initialQuery);
      setSearch("");
      setMenu(false);
      closeDetail();
      window.scrollTo(0, 0);
    };
    window.addEventListener("hashchange", change);
    return () => {
      window.removeEventListener("hashchange", change);
      detailRequest.current?.abort();
    };
  }, [closeDetail]);
  useEffect(() => {
    const id = setTimeout(
      () => setQuery((q) => changeQuery(q, { search })),
      300,
    );
    return () => clearTimeout(id);
  }, [search]);
  const live = useBroker(token, route, query, topicTab, refresh, expired),
    o = live.overview,
    c = live.configuration,
    last = live.history.at(-1);
  useEffect(() => {
    document.title = `${labels[route]} · rmqtt`;
  }, [route]);
  async function openSession(id: string) {
    detailRequest.current?.abort();
    const controller = new AbortController();
    detailRequest.current = controller;
    setDetailId(id);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    try {
      const record = await api<Session>(
        `/api/v1/session?client_id=${encodeURIComponent(id)}`,
        token,
        controller.signal,
      );
      if (!controller.signal.aborted) setDetail(record);
    } catch (error) {
      if (!controller.signal.aborted) {
        if (error instanceof ApiError && error.status === 401) expired();
        else setDetailError(errorMessage(error));
      }
    } finally {
      if (!controller.signal.aborted) setDetailLoading(false);
    }
  }
  const status = live.error
    ? "连接异常"
    : live.loading
      ? "正在同步"
      : o?.ready
        ? "已连接"
        : "连接中";
  const topicPage =
    topicTab === "subscriptions" ? live.subscriptions : live.retained;
  const filter = (
    <div className="flex flex-wrap items-center gap-3 px-5 py-4">
      <div className="relative w-full sm:max-w-xs [&>svg]:pointer-events-none [&>svg]:absolute [&>svg]:top-2.5 [&>svg]:left-3 [&>svg]:size-4 [&>svg]:text-muted-foreground [&_input]:pl-9">
        <Icon name="search" />
        <Input
          aria-label={route === "connections" ? "筛选连接" : "筛选主题"}
          placeholder={
            route === "connections"
              ? "搜索 Client ID 或身份"
              : "搜索 Topic 或 Client ID"
          }
          value={search}
          onChange={(e) => setSearch(boundedSearch(e.target.value))}
          maxLength={256}
        />
      </div>
      {route === "connections" && (
        <NativeSelect
          aria-label="筛选 MQTT 版本"
          value={query.protocol}
          onChange={(e) =>
            setQuery((q) => changeQuery(q, { protocol: e.target.value }))
          }
        >
          <option value="">全部协议版本</option>
          <option value="5.0">MQTT 5.0</option>
          <option value="3.1.1">MQTT 3.1.1</option>
        </NativeSelect>
      )}
      <span className="ml-auto hidden text-[10px] text-muted-foreground xl:inline">
        真实 Broker 数据 · 每 5 秒更新
      </span>
    </div>
  );
  return (
    <>
      <aside
        className={`fixed inset-y-0 left-0 z-40 flex w-56 flex-col border-r bg-card transition-transform md:visible md:translate-x-0 ${menu ? "visible translate-x-0" : "invisible -translate-x-full"}`}
      >
        <Brand />
        <div className="mx-4 mb-6 flex items-center gap-3 rounded-lg border bg-muted/20 p-3 [&_b]:text-xs [&_b]:font-semibold [&_small]:mt-1 [&_small]:block [&_small]:text-[10px] [&_small]:text-muted-foreground">
          <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-accent font-semibold text-primary">
            R
          </span>
          <span>
            <b>当前 Broker</b>
            <small>单节点实例 · 实时数据</small>
          </span>
        </div>
        <nav aria-label="主导航">
          {(["工作台", "管理", "扩展"] as const).map((group, i) => (
            <React.Fragment key={group}>
              <div className="mt-5 mb-2 px-6 text-[10px] tracking-widest text-muted-foreground">
                {group}
              </div>
              {(i === 0
                ? ["overview", "connections", "topics"]
                : i === 1
                  ? ["security", "listeners", "configuration", "logs"]
                  : ["rules"]
              ).map((id) => (
                <a
                  key={id}
                  href={`#${id}`}
                  className={`mx-3 my-1 flex h-11 items-center gap-3 rounded-lg px-3 text-[13px] transition-colors ${route === id ? "bg-accent font-semibold text-primary" : "text-muted-foreground hover:bg-muted"}`}
                  aria-current={route === id ? "page" : undefined}
                >
                  <Icon name={id} />
                  <span>{labels[id as Route]}</span>
                  {id === "logs" && (
                    <span className="ml-auto rounded bg-muted px-1.5 py-0.5 text-[9px] text-muted-foreground">
                      未接入
                    </span>
                  )}
                </a>
              ))}
            </React.Fragment>
          ))}
        </nav>
        <div className="mx-4 mt-auto flex items-start gap-2 border-t py-5 text-xs text-muted-foreground [&_small]:mt-1 [&_small]:block [&_small]:text-[9px] [&_b]:ml-1 [&_b]:text-[10px] [&_b]:font-normal">
          <span className="mt-1 size-1.5 shrink-0 rounded-full bg-emerald-500 ring-4 ring-emerald-50" />
          <div>
            rmqtt <b>{o?.instance.version ?? "—"}</b>
            <small>Rust · Tokio · rustls</small>
          </div>
          <span className="ml-auto rounded border px-1 py-0.5 text-[9px]">
            会话只读
          </span>
        </div>
      </aside>
      {menu && (
        <Button
          variant="outline"
          className="fixed inset-0 z-30 h-auto w-auto rounded-none border-0 bg-slate-950/20 md:hidden"
          aria-label="收起导航"
          onClick={() => setMenu(false)}
        />
      )}
      <div className="min-w-0 md:ml-56">
        <header className="flex h-[70px] items-center justify-between gap-3 border-b bg-card px-4 sm:px-8">
          <Button
            variant="outline"
            className="size-8 shrink-0 border-0 bg-transparent p-0 text-muted-foreground shadow-none hover:bg-muted md:hidden"
            aria-label="展开导航"
            onClick={() => setMenu((x) => !x)}
          >
            <Icon name="menu" />
          </Button>
          <div className="flex min-w-0 items-center gap-2 text-[10px] text-muted-foreground sm:gap-3 [&_b]:max-w-[130px] [&_b]:truncate [&_b]:font-medium [&_b]:text-foreground sm:[&_b]:max-w-xs">
            工作空间 <span>/</span>
            <b className="font-mono text-[11px]">
              {o?.instance.listen ?? "当前实例"}
            </b>
            <span
              className={`size-1.5 shrink-0 rounded-full ${live.error ? "bg-amber-500" : "bg-emerald-500"}`}
            />
          </div>
          <div className="flex shrink-0 items-center gap-2 sm:gap-4">
            <span
              className={`rounded-md border px-2 py-1 text-[10px] ${live.error ? "border-orange-200 bg-orange-50 text-orange-700" : "border-emerald-100 bg-emerald-50 text-primary"}`}
            >
              {status}
            </span>
            <Button
              variant="outline"
              className="h-8 gap-2 rounded-md px-3 text-xs font-medium"
              onClick={logout}
            >
              退出登录
            </Button>
          </div>
        </header>
        <main className="mx-auto min-h-[calc(100vh-130px)] max-w-[1640px] px-4 py-6 sm:px-8 sm:py-8">
          <Heading
            title={labels[route]}
            eyebrow={
              route === "overview"
                ? "WORKSPACE / OVERVIEW"
                : `BROKER / ${route.toUpperCase()}`
            }
            description={
              route === "overview"
                ? "了解 Broker 的连接、消息流量与运行状态。"
                : route === "rules"
                  ? "管理消息处理规则、Webhook 连接器和出站动作。"
                  : "当前实例的真实运行数据与只读配置。"
            }
          >
            <Button
              variant="outline"
              className="h-8 gap-2 rounded-md px-3 text-xs font-medium"
              onClick={() => setRefresh((x) => x + 1)}
              disabled={live.loading}
              aria-label="刷新数据"
            >
              <Icon name="refresh" />
              <span className="hidden sm:inline">刷新数据</span>
            </Button>
          </Heading>
          {live.error && (
            <div
              className="mb-4 rounded-lg border border-orange-200 bg-orange-50 px-4 py-3 text-xs leading-relaxed break-words text-orange-800 [&_strong]:font-medium [&_span]:mt-1 [&_span]:block [&_span]:text-[11px]"
              role="alert"
            >
              <strong>{live.error}</strong>
              {live.updated && (
                <span>
                  {" "}
                  下方保留最近成功采样的数据，已过期 ·{" "}
                  {new Date(live.updated).toLocaleTimeString()}
                </span>
              )}
            </div>
          )}
          {live.loading && !o && (
            <div className="rounded-xl border bg-card shadow-none">
              <Empty>正在连接服务…</Empty>
            </div>
          )}
          {route === "overview" && o && (
            <>
              <div className="mb-5 grid grid-cols-2 gap-3 xl:grid-cols-4 xl:gap-4">
                {[
                  [
                    "在线会话",
                    o.online_sessions,
                    "",
                    `${o.offline_sessions.toLocaleString()} 个离线会话`,
                    "connections",
                  ],
                  [
                    "消息入站",
                    last
                      ? last.incoming.toLocaleString(undefined, {
                          maximumFractionDigits: 1,
                        })
                      : "—",
                    "条/s",
                    last ? "按两次采样差值计算" : "等待下一个采样点",
                    "down",
                  ],
                  [
                    "消息出站",
                    last
                      ? last.outgoing.toLocaleString(undefined, {
                          maximumFractionDigits: 1,
                        })
                      : "—",
                    "条/s",
                    "包含重传的 PUBLISH 帧",
                    "up",
                  ],
                  [
                    "持久与临时会话",
                    o.online_sessions + o.offline_sessions,
                    "",
                    `${o.subscriptions.toLocaleString()} 条订阅`,
                    "configuration",
                  ],
                ].map(([name, value, unit, note, icon]) => (
                  <Card
                    className="border bg-card shadow-none min-h-32 gap-0 p-4 sm:p-5"
                    key={name}
                  >
                    <div className="flex items-center justify-between gap-2 text-[11px] text-muted-foreground">
                      {name}
                      <span className="grid size-7 place-items-center rounded-lg bg-accent/60 text-primary/70">
                        <Icon name={String(icon)} />
                      </span>
                    </div>
                    <div className="mt-3 mb-3 text-[26px] leading-none font-semibold tracking-tight tabular-nums sm:text-[29px] [&_small]:ml-1 [&_small]:text-[10px] [&_small]:font-normal [&_small]:tracking-normal [&_small]:text-muted-foreground">
                      {typeof value === "number"
                        ? value.toLocaleString()
                        : value}
                      <small>{unit}</small>
                    </div>
                    <div className="text-[10px] leading-relaxed text-muted-foreground">
                      {note}
                    </div>
                  </Card>
                ))}
              </div>
              <div className="mb-5 grid gap-5 xl:grid-cols-[minmax(0,2fr)_minmax(280px,1fr)]">
                <Panel title="消息吞吐" extra={<Badge>实时采样</Badge>}>
                  <div className="flex gap-8 px-5 pt-4 [&_small]:mb-1 [&_small]:block [&_small]:text-[10px] [&_small]:text-muted-foreground [&_b]:text-base [&_b]:font-semibold [&_b>span]:ml-1 [&_b>span]:text-[10px] [&_b>span]:font-normal [&_b>span]:text-muted-foreground">
                    <div>
                      <small>
                        <span className="mr-1.5 inline-block h-0.5 w-3 rounded bg-primary align-middle" />
                        消息入站
                      </small>
                      <b>
                        {last
                          ? last.incoming.toLocaleString(undefined, {
                              maximumFractionDigits: 1,
                            })
                          : "—"}
                        <span>条/s</span>
                      </b>
                    </div>
                    <div>
                      <small>
                        <span className="mr-1.5 inline-block h-0.5 w-3 rounded bg-[#91aaca] align-middle" />
                        消息出站
                      </small>
                      <b>
                        {last
                          ? last.outgoing.toLocaleString(undefined, {
                              maximumFractionDigits: 1,
                            })
                          : "—"}
                        <span>条/s</span>
                      </b>
                    </div>
                  </div>
                  <Traffic history={live.history} />
                  <div className="mx-5 flex flex-wrap gap-4 border-t py-3 text-[10px] text-muted-foreground">
                    <span>最近 {live.history.length} 个采样 · 最多 60 个</span>
                    <span>进程重启后计数清零</span>
                  </div>
                </Panel>
                <Panel title="实例信息">
                  <div className="px-5 pt-3">
                    <div className="flex items-center gap-3 border-b pb-4 [&_b]:text-xs [&_b]:font-semibold [&_small]:mt-1 [&_small]:block [&_small]:text-[10px] [&_small]:text-muted-foreground [&_[data-slot=badge]]:ml-auto">
                      <span className="grid size-9 shrink-0 place-items-center rounded-lg border bg-accent/40 text-primary/70">
                        <Icon name="listeners" />
                      </span>
                      <div>
                        <b>{o.instance.listen}</b>
                        <small>单节点 / 只读管理</small>
                      </div>
                      <Badge active={!live.error}>{status}</Badge>
                    </div>
                    <KeyValues
                      items={[
                        ["Broker 版本", o.instance.version],
                        ["运行时长", duration(o.uptime_seconds)],
                        ["连接入口", o.instance.transport],
                        ["会话存储", o.instance.storage],
                        ["协议版本", o.instance.protocol],
                      ]}
                    />
                    <div className="flex flex-wrap gap-2 border-t py-4">
                      <Badge active={o.instance.tls}>
                        {o.instance.tls ? "TLS 已启用" : "无 TLS"}
                      </Badge>
                      <Badge active={o.instance.mtls}>
                        {o.instance.mtls ? "强制 mTLS" : "未启用 mTLS"}
                      </Badge>
                    </div>
                  </div>
                </Panel>
              </div>
              <div className="mb-5 grid gap-5 xl:grid-cols-[minmax(0,2fr)_minmax(280px,1fr)]">
                <Panel title="状态与资源">
                  <div className="grid gap-5 p-5 sm:grid-cols-3">
                    <div>
                      <span className="text-[10px] text-muted-foreground">
                        CPU 使用率
                      </span>
                      <div className="my-2 text-xl font-semibold tracking-tight">
                        —
                      </div>
                      <span className="text-[10px] text-muted-foreground">
                        尚未采集
                      </span>
                    </div>
                    <div>
                      <span className="text-[10px] text-muted-foreground">
                        进程 RSS
                      </span>
                      <div className="my-2 text-xl font-semibold tracking-tight">
                        —
                      </div>
                      <span className="text-[10px] text-muted-foreground">
                        尚未采集
                      </span>
                    </div>
                    <div>
                      <span className="text-[10px] text-muted-foreground">
                        模型状态内存
                      </span>
                      <div className="my-2 text-xl font-semibold tracking-tight">
                        {bytes(o.state_bytes)}
                      </div>
                      <span className="text-[10px] text-muted-foreground">
                        逻辑状态字节，非进程 RSS
                      </span>
                    </div>
                  </div>
                </Panel>
                <Panel
                  title="安全防护"
                  extra={
                    <a
                      className="h-auto gap-1 border-0 bg-transparent p-0 text-xs font-medium text-primary shadow-none hover:bg-transparent hover:underline"
                      href="#security"
                    >
                      查看配置 →
                    </a>
                  }
                >
                  <div className="px-5 py-3">
                    <SecuritySummary config={c} />
                  </div>
                </Panel>
              </div>
              <Panel title="进程累计流量">
                <div className="p-5">
                  <KeyValues
                    items={[
                      ["入站 PUBLISH 帧", o.messages_in_total.toLocaleString()],
                      [
                        "出站 PUBLISH 帧",
                        o.messages_out_total.toLocaleString(),
                      ],
                      ["入站 MQTT 字节", bytes(o.bytes_in_total)],
                      ["出站 MQTT 字节", bytes(o.bytes_out_total)],
                      ["保留消息", o.retained_messages.toLocaleString()],
                    ]}
                  />
                  <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
                    字节计数不含 TCP / TLS / WebSocket
                    封装。读取为最终一致视图；统计包含重传，不等同业务去重消息数。延迟指标尚未采集。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "connections" && (
            <>
              <div className="mb-5 flex flex-wrap items-center gap-4 rounded-lg border border-emerald-100 bg-emerald-50/50 px-5 py-4 text-xs text-emerald-800 [&_small]:text-[10px] [&_small]:text-emerald-700/60 sm:[&_small]:ml-auto">
                <Icon name="connections" />
                在线会话 <b>{o?.online_sessions.toLocaleString() ?? "—"}</b>
                <span>
                  离线会话 <b>{o?.offline_sessions.toLocaleString() ?? "—"}</b>
                </span>
                <small>已提交的会话状态，非 TCP 连接数</small>
              </div>
              <div className="rounded-xl border bg-card shadow-none overflow-hidden">
                <div className="flex gap-5 border-b px-5 [&_button]:h-12 [&_button]:rounded-none [&_button]:border-0 [&_button]:bg-transparent [&_button]:px-0 [&_button]:text-xs [&_button]:shadow-none">
                  {[
                    ["", "全部会话"],
                    ["online", "在线会话"],
                    ["offline", "离线会话"],
                  ].map(([value, name]) => (
                    <Button
                      variant="outline"
                      key={value}
                      aria-pressed={query.state === value}
                      className={
                        query.state === value
                          ? "font-semibold text-primary underline decoration-2 underline-offset-[17px]"
                          : ""
                      }
                      onClick={() =>
                        setQuery((q) => changeQuery(q, { state: value }))
                      }
                    >
                      {name}
                    </Button>
                  ))}
                </div>
                {filter}
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Client ID</TableHead>
                        <TableHead>状态</TableHead>
                        <TableHead>协议</TableHead>
                        <TableHead>认证身份</TableHead>
                        <TableHead>订阅</TableHead>
                        <TableHead>离线排队</TableHead>
                        <TableHead>详情</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {live.sessions?.items.map((s) => (
                        <TableRow key={s.client_id}>
                          <TableCell className="font-mono text-[11px]">
                            {s.client_id}
                          </TableCell>
                          <TableCell>
                            <Badge active={s.online}>
                              {s.online ? "在线" : "离线"}
                            </Badge>
                          </TableCell>
                          <TableCell>{protocolLabel(s.protocol)}</TableCell>
                          <TableCell className="font-mono text-[11px]">
                            {s.principal || "—"}
                          </TableCell>
                          <TableCell>{s.subscriptions}</TableCell>
                          <TableCell>{s.queued}</TableCell>
                          <TableCell>
                            <Button
                              variant="outline"
                              className="h-auto gap-1 border-0 bg-transparent p-0 text-xs font-medium text-primary shadow-none hover:bg-transparent hover:underline"
                              aria-label={`查看 ${s.client_id} 详情`}
                              onClick={() => void openSession(s.client_id)}
                            >
                              查看 →
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
                {live.loading && !live.sessions ? (
                  <Empty>正在查询会话…</Empty>
                ) : live.sessions?.items.length === 0 ? (
                  <Empty>没有符合条件的会话</Empty>
                ) : !live.sessions ? (
                  <Empty>暂时无法读取会话</Empty>
                ) : null}
                {live.sessions && (
                  <Paging
                    data={live.sessions}
                    onPage={(offset) =>
                      setQuery((q) => changeQuery(q, { offset }))
                    }
                  />
                )}
              </div>
            </>
          )}
          {route === "topics" && (
            <div className="rounded-xl border bg-card shadow-none overflow-hidden">
              <div className="flex gap-5 border-b px-5 [&_button]:h-12 [&_button]:rounded-none [&_button]:border-0 [&_button]:bg-transparent [&_button]:px-0 [&_button]:text-xs [&_button]:shadow-none">
                {[
                  ["subscriptions", "订阅过滤器"],
                  ["retained", "保留消息"],
                ].map(([id, name]) => (
                  <Button
                    variant="outline"
                    key={id}
                    aria-pressed={topicTab === id}
                    className={
                      topicTab === id
                        ? "font-semibold text-primary underline decoration-2 underline-offset-[17px]"
                        : ""
                    }
                    onClick={() => {
                      setTopicTab(id);
                      setQuery(initialQuery);
                      setSearch("");
                    }}
                  >
                    {name}
                  </Button>
                ))}
              </div>
              {filter}
              <div className="overflow-x-auto">
                {topicTab === "subscriptions" ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Topic Filter</TableHead>
                        <TableHead>Client ID</TableHead>
                        <TableHead>QoS</TableHead>
                        <TableHead>No Local</TableHead>
                        <TableHead>RAP</TableHead>
                        <TableHead>RH</TableHead>
                        <TableHead>Subscription ID</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {live.subscriptions?.items.map((s) => (
                        <TableRow key={`${s.client_id}/${s.filter}`}>
                          <TableCell className="font-mono text-[11px]">
                            {s.filter}
                          </TableCell>
                          <TableCell className="font-mono text-[11px]">
                            {s.client_id}
                          </TableCell>
                          <TableCell>{s.qos}</TableCell>
                          <TableCell>{s.no_local ? "是" : "否"}</TableCell>
                          <TableCell>
                            {s.retain_as_published ? "是" : "否"}
                          </TableCell>
                          <TableCell>{s.retain_handling}</TableCell>
                          <TableCell>{s.identifier ?? "—"}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Topic</TableHead>
                        <TableHead>QoS</TableHead>
                        <TableHead>Payload 大小</TableHead>
                        <TableHead>过期时间</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {live.retained?.items.map((r) => (
                        <TableRow key={r.topic}>
                          <TableCell className="font-mono text-[11px]">
                            {r.topic}
                          </TableCell>
                          <TableCell>{r.qos}</TableCell>
                          <TableCell>{bytes(r.payload_bytes)}</TableCell>
                          <TableCell>
                            {r.expires_at_millis === null
                              ? "无过期时间"
                              : new Date(r.expires_at_millis).toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </div>
              {live.loading && !topicPage ? (
                <Empty>正在查询主题元数据…</Empty>
              ) : (topicPage?.items.length ?? 0) === 0 ? (
                <Empty>
                  {live.error ? "暂时无法读取数据" : "没有符合条件的记录"}
                </Empty>
              ) : null}
              {topicPage && (
                <Paging
                  data={topicPage}
                  onPage={(offset) =>
                    setQuery((q) => changeQuery(q, { offset }))
                  }
                />
              )}
              <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground mx-4 mb-4">
                只展示元数据，不读取业务消息 Payload。RAP = Retain As
                Published；RH = Retain Handling。活跃 Topic 历史尚未采集。
              </div>
            </div>
          )}
          {route === "security" && c && (
            <>
              <div className="mb-5 grid gap-4 lg:grid-cols-3">
                {[
                  [
                    "MQTT 身份认证",
                    c.instance.auth_policy
                      ? "本地策略已加载"
                      : "默认认证提供者",
                    "认证提供者状态来自实际启动配置；不显示密码或哈希。",
                  ],
                  [
                    "认证准入",
                    `${c.limits.auth_rate_per_second.toLocaleString()} 次 / 秒`,
                    c.limits.auth_rate_per_second === 0
                      ? "速率限制关闭，握手与认证任务并发限制仍生效。"
                      : `突发 ${c.limits.auth_burst} · 并发任务 ${c.limits.max_auth_tasks}`,
                  ],
                  [
                    "传输信任",
                    c.instance.mtls
                      ? "强制 mTLS"
                      : c.instance.tls
                        ? "TLS"
                        : "明文传输",
                    "证书信任校验与 MQTT 身份授权相互独立。",
                  ],
                ].map(([title, value, note]) => (
                  <Card
                    className="border bg-card shadow-none gap-3 p-5 [&_h3]:text-lg [&_h3]:font-semibold [&_p]:text-xs [&_p]:leading-relaxed [&_p]:text-muted-foreground"
                    key={title}
                  >
                    <Badge>{title}</Badge>
                    <h3>{value}</h3>
                    <p>{note}</p>
                  </Card>
                ))}
              </div>
              <Panel title="当前安全配置">
                <div className="p-5">
                  <SecuritySummary config={c} />
                  <KeyValues
                    items={[
                      ["认证任务上限", c.limits.max_auth_tasks],
                      ["未完成握手上限", c.limits.max_handshakes],
                      [
                        "认证限速",
                        c.limits.auth_rate_per_second === 0
                          ? "已关闭"
                          : `${c.limits.auth_rate_per_second} 次 / 秒`,
                      ],
                      ["突发容量", c.limits.auth_burst],
                      [
                        "Enhanced AUTH",
                        c.instance.enhanced_auth ? "已配置" : "未配置",
                      ],
                    ]}
                  />
                  <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
                    此处显示当前认证设置。客户端连接凭据与 Dashboard
                    访问令牌分别管理。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "listeners" && o && (
            <>
              <div className="mb-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                {["TCP", "TLS", "WS", "WSS"].map((type) => (
                  <Card
                    className={`flex flex-col gap-3 rounded-xl border bg-card p-5 ${o.instance.transport.toUpperCase() !== type ? "border-t-2 border-t-slate-200 text-muted-foreground" : "border-t-2 border-t-emerald-300"}`}
                    key={type}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="grid size-9 place-items-center rounded-lg bg-accent text-primary/80">
                        <Icon name="listeners" />
                      </span>
                      <Badge
                        active={o.instance.transport.toUpperCase() === type}
                      >
                        {o.instance.transport.toUpperCase() === type
                          ? "当前入口"
                          : "未启用"}
                      </Badge>
                    </div>
                    <h3>{type}</h3>
                    <div className="my-3 text-base font-semibold break-all">
                      {o.instance.transport.toUpperCase() === type
                        ? o.instance.listen
                        : "—"}
                    </div>
                  </Card>
                ))}
              </div>
              <Panel title="监听信息">
                <div className="p-5">
                  <KeyValues
                    items={[
                      ["监听地址", o.instance.listen],
                      ["传输方式", o.instance.transport],
                      ["协议范围", o.instance.protocol],
                      ["TLS", o.instance.tls ? "已启用" : "未启用"],
                      ["强制客户端证书", o.instance.mtls ? "是" : "否"],
                    ]}
                  />
                  <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
                    MQTT 客户端请使用上方监听地址连接。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "configuration" && c && (
            <Panel title="启动参数">
              <div className="p-5">
                {Object.entries(c.limits).map(([key, value]) => (
                  <div
                    className="grid gap-3 border-b py-4 last:border-0 sm:grid-cols-[1fr_220px] [&_b]:text-xs [&_b]:font-medium [&_p]:mt-1 [&_p]:text-[10px] [&_p]:text-muted-foreground"
                    key={key}
                  >
                    <div>
                      <b>{limitLabels[key] ?? key}</b>
                    </div>
                    <div className="rounded-md border bg-muted/50 px-3 py-2 font-mono text-xs text-primary">
                      {value.toLocaleString()}
                      {key.endsWith("_ms") ? " ms" : ""}
                    </div>
                  </div>
                ))}
                <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
                  当前配置仅供查看。修改启动参数后需重启服务生效。
                </div>
              </div>
            </Panel>
          )}
          {route === "rules" && <RulesPage token={token} expired={expired} />}
          {route === "logs" && (
            <Panel title="事件记录">
              <Empty>
                事件历史尚未接入。请使用 Broker 进程日志排查运行问题。
              </Empty>
            </Panel>
          )}
          {!o && !live.loading && !live.error && (
            <Empty>Broker 暂无可用数据</Empty>
          )}
        </main>
        <footer className="mx-auto flex max-w-[1640px] flex-col justify-between gap-2 px-4 py-5 text-[10px] text-muted-foreground sm:flex-row sm:px-8">
          <span>rmqtt console / 单节点 MQTT Broker</span>
          <span>
            {live.updated
              ? `最近成功采样 ${new Date(live.updated).toLocaleTimeString()} · 最终一致视图`
              : "等待采样"}{" "}
            · 会话只读 / 规则配置可管理
          </span>
        </footer>
      </div>
      {detailId && (
        <Drawer
          title="连接与会话详情"
          subtitle={detailId}
          onClose={closeDetail}
        >
          {detailLoading ? (
            <Empty>正在读取会话…</Empty>
          ) : detailError ? (
            <div
              className="mb-4 rounded-lg border border-orange-200 bg-orange-50 px-4 py-3 text-xs leading-relaxed break-words text-orange-800 [&_strong]:font-medium [&_span]:mt-1 [&_span]:block [&_span]:text-[11px]"
              role="alert"
            >
              {detailError}
            </div>
          ) : (
            detail && (
              <>
                <div className="flex flex-wrap gap-2">
                  <Badge active={detail.online}>
                    {detail.online ? "在线" : "离线"}
                  </Badge>
                  <Badge>{protocolLabel(detail.protocol)}</Badge>
                </div>
                <h3 className="mt-6 border-b pb-3 text-xs font-semibold">
                  客户端
                </h3>
                <KeyValues
                  items={[
                    ["Client ID", detail.client_id],
                    ["认证身份", detail.principal || "—"],
                    ["持久会话", detail.persistent ? "是" : "否"],
                    ["订阅数量", detail.subscriptions],
                  ]}
                />
                <h3 className="mt-6 border-b pb-3 text-xs font-semibold">
                  会话与消息
                </h3>
                <KeyValues
                  items={[
                    ["排队消息", detail.queued],
                    ["出站 Inflight", detail.inflight],
                    [
                      "Receive Maximum",
                      detail.protocol === "3.1.1"
                        ? "不适用（MQTT 3.1.1）"
                        : (detail.receive_maximum ?? "未协商"),
                    ],
                    [
                      "会话有效期",
                      detail.protocol === "3.1.1"
                        ? detail.persistent
                          ? "持久保留"
                          : "当前连接"
                        : detail.session_expiry_interval === 4294967295
                          ? "不过期"
                          : `${detail.session_expiry_interval} 秒`,
                    ],
                  ]}
                />
                <div className="mt-4 rounded-lg border bg-muted/60 p-3 text-[11px] leading-relaxed text-muted-foreground">
                  详情为本次查询快照，不包含业务消息内容。当前版本不提供强制断开与删除操作。
                </div>
              </>
            )
          )}
        </Drawer>
      )}
    </>
  );
}
const limitLabels: Record<string, string> = {
  max_connections: "最大连接数",
  max_sessions: "最大会话数",
  max_packet_size: "最大 MQTT 报文大小（字节）",
  max_subscriptions: "单会话订阅上限",
  max_retained: "保留消息上限",
  max_inflight: "QoS Inflight 上限",
  outbound_capacity: "离线 / 出站队列容量",
  max_handshakes: "握手并发上限",
  max_auth_tasks: "认证任务并发上限",
  auth_rate_per_second: "每秒认证令牌补充",
  auth_burst: "认证突发容量",
  handshake_timeout_ms: "握手超时",
  write_timeout_ms: "写入超时",
  topic_alias_maximum: "Topic Alias 上限",
};
function SecuritySummary({ config }: { config: Configuration | null }) {
  if (!config) return <span className="text-muted-foreground">等待配置…</span>;
  return (
    <>
      {[
        ["本地认证策略", config.instance.auth_policy ? "已加载" : "未加载"],
        ["传输加密", config.instance.tls ? "TLS 已启用" : "未启用"],
        ["客户端证书", config.instance.mtls ? "强制 mTLS" : "未启用"],
        ["管理操作", "会话只读 / 规则配置写入 · Bearer 令牌"],
      ].map(([label, value]) => (
        <div
          className="my-3 flex items-center gap-2 text-xs text-foreground/70 [&_span:last-child]:ml-auto [&_span:last-child]:text-[10px]"
          key={label}
        >
          <span className="grid size-4 shrink-0 place-items-center rounded-full border border-emerald-200 bg-emerald-50 text-primary">
            ·
          </span>
          {label}
          <span className="text-muted-foreground">{value}</span>
        </div>
      ))}
    </>
  );
}
export function App() {
  const [auth, dispatch] = useReducer(authReducer, {
    token: null,
    error: null,
  });
  const logout = useCallback(() => dispatch({ type: "logout" }), []),
    expired = useCallback(() => dispatch({ type: "expired" }), []);
  return auth.token ? (
    <Console token={auth.token} logout={logout} expired={expired} />
  ) : (
    <Login
      key={auth.error ?? "login"}
      error={auth.error}
      onLogin={(token) => dispatch({ type: "login", token })}
    />
  );
}
