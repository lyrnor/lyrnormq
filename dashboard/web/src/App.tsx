import React, {
  useCallback,
  useEffect,
  useReducer,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { api, ApiError, errorMessage } from "./api";
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
    <a className="brand" href="#overview" aria-label="rmqtt 首页">
      <span className="brand-mark">
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
    <div className="login-shell">
      <section className="login-panel">
        <Brand />
        <div className="eyebrow">SINGLE NODE / LIVE CONSOLE</div>
        <h1>连接到 Broker</h1>
        <p>使用独立管理令牌，查看当前实例的实时状态。</p>
        <form onSubmit={(e) => void submit(e)}>
          <label htmlFor="token">管理令牌</label>
          <input
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
            <div className="error-box" role="alert">
              {message}
            </div>
          )}
          <button
            className="btn primary"
            disabled={busy || value.trim().length < 32}
          >
            {busy ? "正在验证…" : "进入控制台"}
            <span aria-hidden="true"> →</span>
          </button>
        </form>
        <div className="note">
          令牌仅保留在当前页面内存中，刷新页面后需重新输入。控制台为只读，不修改
          Broker 状态。
        </div>
        <small className="login-footer">Rust · Tokio · rustls</small>
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
  const filter = (
    <div className="filters">
      <div className="search-input">
        <Icon name="search" />
        <input
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
        <select
          aria-label="筛选 MQTT 版本"
          value={query.protocol}
          onChange={(e) =>
            setQuery((q) => changeQuery(q, { protocol: e.target.value }))
          }
        >
          <option value="">全部协议版本</option>
          <option value="5.0">MQTT 5.0</option>
          <option value="3.1.1">MQTT 3.1.1</option>
        </select>
      )}
      <span className="filter-note">真实 Broker 数据 · 每 5 秒更新</span>
    </div>
  );
  return (
    <>
      <aside className={`sidebar ${menu ? "open" : ""}`}>
        <Brand />
        <div className="workspace">
          <span className="workspace-icon">R</span>
          <span>
            <b>当前 Broker</b>
            <small>单节点实例 · 实时数据</small>
          </span>
        </div>
        <nav aria-label="主导航">
          {(["工作台", "管理", "扩展"] as const).map((group, i) => (
            <React.Fragment key={group}>
              <div className="nav-group-title">{group}</div>
              {(i === 0
                ? ["overview", "connections", "topics"]
                : i === 1
                  ? ["security", "listeners", "configuration", "logs"]
                  : ["rules"]
              ).map((id) => (
                <a
                  key={id}
                  href={`#${id}`}
                  className={`nav-link ${route === id ? "active" : ""}`}
                  aria-current={route === id ? "page" : undefined}
                >
                  <Icon name={id} />
                  <span>{labels[id as Route]}</span>
                  {(id === "rules" || id === "logs") && (
                    <span className="nav-tag">未接入</span>
                  )}
                </a>
              ))}
            </React.Fragment>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <span className="runtime-dot" />
          <div>
            rmqtt <b>{o?.instance.version ?? "—"}</b>
            <small>Rust · Tokio · rustls</small>
          </div>
          <span className="mini-badge">只读</span>
        </div>
      </aside>
      {menu && (
        <button
          className="mobile-backdrop"
          aria-label="收起导航"
          onClick={() => setMenu(false)}
        />
      )}
      <div className="shell">
        <header className="topbar">
          <button
            className="icon-button mobile-menu"
            aria-label="展开导航"
            onClick={() => setMenu((x) => !x)}
          >
            <Icon name="menu" />
          </button>
          <div className="breadcrumb">
            工作空间 <span>/</span>
            <b className="mono">{o?.instance.listen ?? "当前实例"}</b>
            <span className={`live-dot ${live.error ? "offline" : ""}`} />
          </div>
          <div className="top-actions">
            <span
              className={`connection-tag ${live.error ? "disconnected" : ""}`}
            >
              {status}
            </span>
            <button className="btn" onClick={logout}>
              退出登录
            </button>
          </div>
        </header>
        <main>
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
                : "当前实例的真实运行数据与只读配置。"
            }
          >
            <button
              className="btn"
              onClick={() => setRefresh((x) => x + 1)}
              disabled={live.loading}
              aria-label="刷新数据"
            >
              <Icon name="refresh" />
              <span className="hide-mobile">刷新数据</span>
            </button>
          </Heading>
          {live.error && (
            <div className="error-box" role="alert">
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
            <div className="panel">
              <Empty>正在连接管理 API…</Empty>
            </div>
          )}
          {route === "overview" && o && (
            <>
              <div className="cards">
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
                  <div className="card metric" key={name}>
                    <div className="metric-label">
                      {name}
                      <span className="metric-symbol">
                        <Icon name={String(icon)} />
                      </span>
                    </div>
                    <div className="metric-number">
                      {typeof value === "number"
                        ? value.toLocaleString()
                        : value}
                      <small>{unit}</small>
                    </div>
                    <div className="metric-foot">{note}</div>
                  </div>
                ))}
              </div>
              <div className="two-column">
                <Panel title="消息吞吐" extra={<Badge>实时采样</Badge>}>
                  <div className="chart-meta">
                    <div>
                      <small>
                        <span className="legend-mark" />
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
                        <span className="legend-mark blue" />
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
                  <div className="chart-bottom">
                    <span>最近 {live.history.length} 个采样 · 最多 60 个</span>
                    <span>进程重启后计数清零</span>
                  </div>
                </Panel>
                <Panel title="实例信息">
                  <div className="instance-body">
                    <div className="instance-name">
                      <span className="server-icon">
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
                    <div className="instance-tags">
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
              <div className="two-column">
                <Panel title="状态与资源">
                  <div className="resource-row">
                    <div>
                      <span className="resource-label">CPU 使用率</span>
                      <div className="resource-value">—</div>
                      <span className="resource-note">尚未采集</span>
                    </div>
                    <div>
                      <span className="resource-label">进程 RSS</span>
                      <div className="resource-value">—</div>
                      <span className="resource-note">尚未采集</span>
                    </div>
                    <div>
                      <span className="resource-label">模型状态内存</span>
                      <div className="resource-value">
                        {bytes(o.state_bytes)}
                      </div>
                      <span className="resource-note">
                        逻辑状态字节，非进程 RSS
                      </span>
                    </div>
                  </div>
                </Panel>
                <Panel
                  title="安全防护"
                  extra={
                    <a className="text-link" href="#security">
                      查看配置 →
                    </a>
                  }
                >
                  <div className="security-status">
                    <SecuritySummary config={c} />
                  </div>
                </Panel>
              </div>
              <Panel title="进程累计流量">
                <div className="panel-body">
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
                  <div className="note">
                    字节计数不含 TCP / TLS / WebSocket
                    封装。读取为最终一致视图；统计包含重传，不等同业务去重消息数。延迟指标尚未采集。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "connections" && (
            <>
              <div className="summary-strip">
                <Icon name="connections" />
                在线会话 <b>{o?.online_sessions.toLocaleString() ?? "—"}</b>
                <span>
                  离线会话 <b>{o?.offline_sessions.toLocaleString() ?? "—"}</b>
                </span>
                <small>已提交的会话状态，非 TCP 连接数</small>
              </div>
              <div className="panel table-panel">
                <div className="table-tab">
                  {[
                    ["", "全部会话"],
                    ["online", "在线会话"],
                    ["offline", "离线会话"],
                  ].map(([value, name]) => (
                    <button
                      key={value}
                      className={query.state === value ? "active" : ""}
                      onClick={() =>
                        setQuery((q) => changeQuery(q, { state: value }))
                      }
                    >
                      {name}
                    </button>
                  ))}
                </div>
                {filter}
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Client ID</th>
                        <th>状态</th>
                        <th>协议</th>
                        <th>认证身份</th>
                        <th>订阅</th>
                        <th>离线排队</th>
                        <th>详情</th>
                      </tr>
                    </thead>
                    <tbody>
                      {live.sessions?.items.map((s) => (
                        <tr key={s.client_id}>
                          <td className="mono">{s.client_id}</td>
                          <td>
                            <Badge active={s.online}>
                              {s.online ? "在线" : "离线"}
                            </Badge>
                          </td>
                          <td>{protocolLabel(s.protocol)}</td>
                          <td className="mono">{s.principal || "—"}</td>
                          <td>{s.subscriptions}</td>
                          <td>{s.queued}</td>
                          <td>
                            <button
                              className="text-link"
                              aria-label={`查看 ${s.client_id} 详情`}
                              onClick={() => void openSession(s.client_id)}
                            >
                              查看 →
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
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
            <div className="panel table-panel">
              <div className="table-tab">
                {[
                  ["subscriptions", "订阅过滤器"],
                  ["retained", "保留消息"],
                ].map(([id, name]) => (
                  <button
                    key={id}
                    className={topicTab === id ? "active" : ""}
                    onClick={() => {
                      setTopicTab(id);
                      setQuery(initialQuery);
                      setSearch("");
                    }}
                  >
                    {name}
                  </button>
                ))}
              </div>
              {filter}
              <div className="table-wrap">
                {topicTab === "subscriptions" ? (
                  <table>
                    <thead>
                      <tr>
                        <th>Topic Filter</th>
                        <th>Client ID</th>
                        <th>QoS</th>
                        <th>No Local</th>
                        <th>RAP</th>
                        <th>RH</th>
                        <th>Subscription ID</th>
                      </tr>
                    </thead>
                    <tbody>
                      {live.subscriptions?.items.map((s) => (
                        <tr key={`${s.client_id}/${s.filter}`}>
                          <td className="mono">{s.filter}</td>
                          <td className="mono">{s.client_id}</td>
                          <td>{s.qos}</td>
                          <td>{s.no_local ? "是" : "否"}</td>
                          <td>{s.retain_as_published ? "是" : "否"}</td>
                          <td>{s.retain_handling}</td>
                          <td>{s.identifier ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <table>
                    <thead>
                      <tr>
                        <th>Topic</th>
                        <th>QoS</th>
                        <th>Payload 大小</th>
                        <th>过期时间</th>
                      </tr>
                    </thead>
                    <tbody>
                      {live.retained?.items.map((r) => (
                        <tr key={r.topic}>
                          <td className="mono">{r.topic}</td>
                          <td>{r.qos}</td>
                          <td>{bytes(r.payload_bytes)}</td>
                          <td>
                            {r.expires_at_millis === null
                              ? "无过期时间"
                              : new Date(r.expires_at_millis).toLocaleString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
              {live.loading && !(live.subscriptions || live.retained) ? (
                <Empty>正在查询主题元数据…</Empty>
              ) : ((topicTab === "subscriptions"
                  ? live.subscriptions
                  : live.retained
                )?.items.length ?? 0) === 0 ? (
                <Empty>
                  {live.error ? "暂时无法读取数据" : "没有符合条件的记录"}
                </Empty>
              ) : null}
              {(live.subscriptions || live.retained) && (
                <Paging
                  data={
                    (topicTab === "subscriptions"
                      ? live.subscriptions
                      : live.retained)!
                  }
                  onPage={(offset) =>
                    setQuery((q) => changeQuery(q, { offset }))
                  }
                />
              )}
              <div className="note table-note">
                只展示元数据，不读取业务消息 Payload。RAP = Retain As
                Published；RH = Retain Handling。活跃 Topic 历史尚未采集。
              </div>
            </div>
          )}
          {route === "security" && c && (
            <>
              <div className="detail-grid">
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
                  <section className="card detail-card" key={title}>
                    <Badge>{title}</Badge>
                    <h3>{value}</h3>
                    <p>{note}</p>
                  </section>
                ))}
              </div>
              <Panel title="当前安全配置">
                <div className="panel-body">
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
                  <div className="note">
                    本版不提供用户、ACL 或证书内容查询与修改。Redis / PostgreSQL
                    认证和证书身份映射未接入。管理令牌与 MQTT 客户端认证独立。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "listeners" && o && (
            <>
              <div className="listener-grid">
                {["TCP", "TLS", "WS", "WSS"].map((type) => (
                  <section
                    className={`card detail-card listener-card ${o.instance.transport.toUpperCase() !== type ? "inactive" : ""}`}
                    key={type}
                  >
                    <div className="detail-card-top">
                      <span className="card-icon">
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
                    <div className="detail-card-number">
                      {o.instance.transport.toUpperCase() === type
                        ? o.instance.listen
                        : "—"}
                    </div>
                  </section>
                ))}
              </div>
              <Panel title="监听信息">
                <div className="panel-body">
                  <KeyValues
                    items={[
                      ["监听地址", o.instance.listen],
                      ["传输方式", o.instance.transport],
                      ["协议范围", o.instance.protocol],
                      ["TLS", o.instance.tls ? "已启用" : "未启用"],
                      ["强制客户端证书", o.instance.mtls ? "是" : "否"],
                    ]}
                  />
                  <div className="note">
                    当前进程仅运行一个 MQTT 监听入口。管理 HTTP
                    是独立本机只读入口，不表示支持多个 MQTT 监听器共享状态。
                  </div>
                </div>
              </Panel>
            </>
          )}
          {route === "configuration" && c && (
            <Panel title="启动参数">
              <div className="panel-body">
                {Object.entries(c.limits).map(([key, value]) => (
                  <div className="config-row" key={key}>
                    <div>
                      <b>{limitLabels[key] ?? key}</b>
                      <p className="mono">{key}</p>
                    </div>
                    <div className="config-value">
                      {value.toLocaleString()}
                      {key.endsWith("_ms") ? " ms" : ""}
                    </div>
                  </div>
                ))}
                <div className="note">
                  配置在进程启动时加载。本页仅显示安全的资源限制，不返回密钥、令牌、存储路径或策略内容；修改与热加载尚未实现。
                </div>
              </div>
            </Panel>
          )}
          {(route === "logs" || route === "rules") && (
            <Panel title={labels[route]}>
              <div className="blank-state">
                <div className="blank-icon">
                  <Icon name={route} />
                </div>
                <h2>
                  {route === "logs" ? "事件历史尚未接入" : "规则引擎尚未实现"}
                </h2>
                <p>
                  {route === "logs"
                    ? "管理 API 暂不采集事件历史。请使用 Broker 进程日志排查运行问题。"
                    : "当前 Broker 不执行 SQL 规则。本页保留信息架构入口，后续将先冻结规则与动作契约。"}
                </p>
                <Badge active={false}>不可用</Badge>
              </div>
            </Panel>
          )}
          {!o && !live.loading && !live.error && (
            <Empty>Broker 暂无可用数据</Empty>
          )}
        </main>
        <footer>
          <span>rmqtt console / 单节点 MQTT Broker</span>
          <span>
            {live.updated
              ? `最近成功采样 ${new Date(live.updated).toLocaleTimeString()} · 最终一致视图`
              : "等待采样"}{" "}
            · 只读管理
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
            <div className="error-box" role="alert">
              {detailError}
            </div>
          ) : (
            detail && (
              <>
                <div className="pill-group">
                  <Badge active={detail.online}>
                    {detail.online ? "在线" : "离线"}
                  </Badge>
                  <Badge>{protocolLabel(detail.protocol)}</Badge>
                </div>
                <h3 className="drawer-section">客户端</h3>
                <KeyValues
                  items={[
                    ["Client ID", detail.client_id],
                    ["认证身份", detail.principal || "—"],
                    ["持久会话", detail.persistent ? "是" : "否"],
                    ["订阅数量", detail.subscriptions],
                  ]}
                />
                <h3 className="drawer-section">会话与消息</h3>
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
                <div className="note">
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
  if (!config) return <span className="muted">等待配置…</span>;
  return (
    <>
      {[
        ["本地认证策略", config.instance.auth_policy ? "已加载" : "未加载"],
        ["传输加密", config.instance.tls ? "TLS 已启用" : "未启用"],
        ["客户端证书", config.instance.mtls ? "强制 mTLS" : "未启用"],
        ["管理操作", "只读 · Bearer 令牌"],
      ].map(([label, value]) => (
        <div className="security-line" key={label}>
          <span className="check-circle">·</span>
          {label}
          <span className="muted">{value}</span>
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
