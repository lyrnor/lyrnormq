import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ReactNode,
  type FormEvent,
} from "react";
import { api, ApiError, errorMessage } from "./api";
import { Badge, Drawer, Empty, Panel } from "./components";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "./components/ui/native-select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./components/ui/table";
import { bytes } from "./model";
import {
  SQL_EXAMPLE,
  defaultAction,
  deletionError,
  testEvent,
  validateAction,
  validateConnector,
  validateIdentity,
  validateSql,
  type RuleDefinition,
  type RulesConfig,
  type RulesStatus,
  type WebhookAction,
  type WebhookConnector,
} from "./rules-model";

type Kind = "rules" | "connectors" | "actions";
type Item = RuleDefinition | WebhookConnector | WebhookAction;
type Editing = { kind: Kind; value: Item; fresh: boolean };
const names = { rules: "规则", connectors: "连接器", actions: "出站动作" };
const textarea =
  "min-h-28 w-full rounded-md border bg-card p-3 font-mono text-xs leading-relaxed outline-offset-4 focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30";
const note =
  "rounded-lg border bg-muted/60 p-3 text-xs leading-relaxed text-muted-foreground";
function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="grid gap-2 text-xs font-medium">
      {label}
      {children}
    </label>
  );
}
function Notice({ children }: { children: ReactNode }) {
  return (
    <div
      role="alert"
      className="rounded-lg border border-orange-200 bg-orange-50 px-4 py-3 text-xs leading-relaxed text-orange-900"
    >
      {children}
    </div>
  );
}

export function RulesPage({
  token,
  expired,
}: {
  token: string;
  expired: () => void;
}) {
  const [config, setConfig] = useState<RulesConfig | null>(null),
    [status, setStatus] = useState<RulesStatus | null>(null);
  const [tab, setTab] = useState<Kind>("rules"),
    [loading, setLoading] = useState(true),
    [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null),
    [disabled, setDisabled] = useState(false),
    [notice, setNotice] = useState<string | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null),
    [sampledAt, setSampledAt] = useState<number | null>(null);
  const saving = useRef(false);
  const stopped = status?.running === false;
  const [edit, setEdit] = useState<Editing | null>(null),
    [remove, setRemove] = useState<{ kind: Kind; id: string } | null>(null),
    [test, setTest] = useState<string | null>(null);
  const requests = useRef({
    load: null as AbortController | null,
    poll: null as AbortController | null,
    save: null as AbortController | null,
  });
  const onError = useCallback(
    (cause: unknown) => {
      if (cause instanceof ApiError && cause.status === 401) expired();
      else {
        setError(errorMessage(cause));
        setDisabled(
          cause instanceof ApiError && cause.code === "rules_disabled",
        );
      }
    },
    [expired],
  );
  const load = useCallback(async () => {
    requests.current.load?.abort();
    const controller = new AbortController();
    requests.current.load = controller;
    setLoading(true);
    setError(null);
    try {
      const [next, metrics] = await Promise.all([
        api<RulesConfig>("/api/v1/rules/config", token, controller.signal),
        api<RulesStatus>("/api/v1/rules/status", token, controller.signal),
      ]);
      if (!controller.signal.aborted) {
        setConfig(next);
        setStatus(metrics);
        setDisabled(false);
        setStatusError(null);
        setSampledAt(Date.now());
      }
    } catch (cause) {
      if (!controller.signal.aborted) onError(cause);
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [token, onError]);
  useEffect(() => {
    void load();
    let running = false;
    const poll = async () => {
      if (running || document.hidden) return;
      running = true;
      const controller = new AbortController();
      requests.current.poll = controller;
      try {
        const next = await api<RulesStatus>(
          "/api/v1/rules/status",
          token,
          controller.signal,
        );
        if (!controller.signal.aborted) {
          setStatus(next);
          setStatusError(null);
          setSampledAt(Date.now());
        }
      } catch (cause) {
        if (!controller.signal.aborted) {
          if (cause instanceof ApiError && cause.status === 401) expired();
          else setStatusError(errorMessage(cause));
        }
      } finally {
        running = false;
      }
    };
    const visible = () => {
      if (!document.hidden) void poll();
    };
    const timer = window.setInterval(() => void poll(), 5000);
    document.addEventListener("visibilitychange", visible);
    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", visible);
      Object.values(requests.current).forEach((c) => c?.abort());
    };
  }, [load, token, onError]);
  async function save(next: RulesConfig): Promise<boolean> {
    if (saving.current || stopped) return false;
    saving.current = true;
    requests.current.load?.abort();
    requests.current.save?.abort();
    const controller = new AbortController();
    requests.current.save = controller;
    setBusy(true);
    setLoading(false);
    setError(null);
    setNotice(null);
    try {
      const stored = await api<RulesConfig>(
        "/api/v1/rules/config",
        token,
        controller.signal,
        { method: "PUT", body: next },
      );
      if (controller.signal.aborted) return false;
      setConfig(stored);
      setNotice(
        "配置已保存并应用。更改会替换运行版本，旧版本积压可能被取消并计数。",
      );
      return true;
    } catch (cause) {
      if (!controller.signal.aborted) onError(cause);
      return false;
    } finally {
      saving.current = false;
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  function create() {
    const value =
      tab === "rules"
        ? { id: "", name: "", sql: SQL_EXAMPLE, enabled: true, action_ids: [] }
        : tab === "connectors"
          ? { id: "", name: "", url: "", headers: {} }
          : defaultAction();
    setEdit({ kind: tab, value, fresh: true });
    setNotice(null);
  }
  async function saveEdit(value: Item) {
    if (!config || !edit) return;
    const list = config[edit.kind] as Item[];
    if (edit.fresh && list.some((item) => item.id === value.id))
      throw new Error("ID 已存在，请使用不同 ID");
    if (!edit.fresh && !list.some((item) => item.id === value.id))
      throw new Error("记录已不存在，请刷新后重试");
    const next = {
      ...config,
      [edit.kind]: edit.fresh
        ? [...list, value]
        : list.map((item) => (item.id === value.id ? value : item)),
    };
    if (await save(next)) setEdit(null);
    else
      throw new Error(
        "保存未确认，请刷新配置核对；检查 SQL、参数范围、引用关系或存储状态后重试",
      );
  }
  async function toggle(kind: "rules" | "actions", id: string) {
    if (config)
      await save({
        ...config,
        [kind]: config[kind].map((item) =>
          item.id === id ? { ...item, enabled: !item.enabled } : item,
        ),
      });
  }
  async function confirmRemove() {
    if (!config || !remove) return;
    const issue = deletionError(remove.kind, remove.id, config);
    if (issue) {
      setError(issue);
      return;
    }
    if (
      await save({
        ...config,
        [remove.kind]: config[remove.kind].filter(
          (item) => item.id !== remove.id,
        ),
      })
    )
      setRemove(null);
  }
  function controls(kind: Kind, item: Item) {
    return (
      <>
        <Button
          size="sm"
          variant="ghost"
          disabled={busy}
          aria-label={`编辑 ${item.id}`}
          onClick={() => setEdit({ kind, value: item, fresh: false })}
        >
          编辑
        </Button>
        <Button
          size="sm"
          variant="ghost"
          disabled={busy || stopped}
          aria-label={`删除 ${item.id}`}
          onClick={() => setRemove({ kind, id: item.id })}
        >
          删除
        </Button>
      </>
    );
  }
  const totals = status
    ? ([
        ["接入事件", status.received],
        ["规则命中", status.matched],
        ["排队事件", status.queued_events],
        ["丢弃", status.dropped],
        ["处理失败", status.errors],
      ] as const)
    : [];
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">规则引擎</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            筛选消息、提取数据，并发送到指定的 Webhook。
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          disabled={loading || busy}
          onClick={() => void load()}
        >
          刷新规则配置
        </Button>
      </div>
      {error && (
        <Notice>
          {error}
          {config && (
            <p className="mt-1">显示上次成功读取的数据，请刷新后核对。</p>
          )}
        </Notice>
      )}
      {statusError && (
        <Notice>
          运行统计刷新失败：{statusError}。显示上次成功采样，不能视为实时状态。
        </Notice>
      )}
      {stopped && (
        <Notice>
          规则服务已停止，消息投递已暂停。暂时无法保存配置或测试规则，请联系管理员恢复服务。
        </Notice>
      )}
      {notice && !stopped && (
        <div
          role="status"
          className="rounded-lg border border-emerald-200 bg-emerald-50 p-3 text-xs text-emerald-900"
        >
          {notice}
        </div>
      )}
      {status && (
        <section
          aria-label="规则运行统计"
          className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-6"
        >
          {totals.map(([label, value]) => (
            <div key={label} className="rounded-xl border bg-card p-4">
              <p className="text-xs text-muted-foreground">{label}</p>
              <p className="mt-2 text-xl font-semibold">
                {value.toLocaleString()}
              </p>
            </div>
          ))}
          <div className="rounded-xl border bg-card p-4">
            <p className="text-xs text-muted-foreground">待发送数据量</p>
            <p className="mt-2 text-xl font-semibold">
              {bytes(status.queued_bytes)}
            </p>
          </div>
        </section>
      )}
      {loading && !config ? (
        <Empty>正在读取规则配置…</Empty>
      ) : !config ? (
        <Empty>
          {disabled ? "规则引擎尚未启用" : "暂时无法加载规则配置，请稍后重试"}
        </Empty>
      ) : (
        <Panel
          title="规则配置"
          extra={
            <Button
              size="sm"
              disabled={
                busy ||
                stopped ||
                config[tab].length >=
                  (tab === "rules" ? 128 : tab === "actions" ? 64 : 32)
              }
              onClick={create}
            >
              新建{names[tab]}
            </Button>
          }
        >
          <div
            className="flex flex-wrap gap-2 px-5 pt-4"
            aria-label="规则管理分类"
          >
            {(["rules", "connectors", "actions"] as Kind[]).map((kind) => (
              <Button
                key={kind}
                size="sm"
                variant={tab === kind ? "secondary" : "ghost"}
                aria-pressed={tab === kind}
                onClick={() => setTab(kind)}
              >
                {names[kind]} {config[kind].length}
              </Button>
            ))}
          </div>
          <div className="overflow-x-auto p-4">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID / 名称</TableHead>
                  <TableHead>
                    {tab === "rules"
                      ? "SQL / 动作绑定"
                      : tab === "connectors"
                        ? "Webhook 地址"
                        : "连接器 / 发送策略"}
                  </TableHead>
                  {tab !== "connectors" && <TableHead>状态</TableHead>}
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tab === "rules" &&
                  config.rules.map((rule) => (
                    <TableRow key={rule.id}>
                      <TableCell>
                        <b className="text-xs font-medium">{rule.name}</b>
                        <p className="mt-1 font-mono text-[11px] text-muted-foreground">
                          {rule.id}
                        </p>
                      </TableCell>
                      <TableCell>
                        <pre
                          className="max-w-md truncate font-mono text-[11px]"
                          title={rule.sql}
                        >
                          {rule.sql}
                        </pre>
                        <p className="mt-1 text-xs text-muted-foreground">
                          动作：{rule.action_ids.join("、") || "未绑定"}
                        </p>
                      </TableCell>
                      <TableCell>
                        <Badge active={rule.enabled}>
                          {rule.enabled ? "已启用" : "已停用"}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end gap-1">
                          <Button
                            size="sm"
                            variant="ghost"
                            disabled={stopped}
                            onClick={() => setTest(rule.sql)}
                          >
                            测试
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            disabled={busy || stopped}
                            onClick={() => void toggle("rules", rule.id)}
                          >
                            {rule.enabled ? "停用" : "启用"}
                          </Button>
                          {controls("rules", rule)}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                {tab === "connectors" &&
                  config.connectors.map((connector) => (
                    <TableRow key={connector.id}>
                      <TableCell>
                        <b className="text-xs font-medium">{connector.name}</b>
                        <p className="mt-1 font-mono text-[11px] text-muted-foreground">
                          {connector.id}
                        </p>
                      </TableCell>
                      <TableCell>
                        <p className="max-w-sm truncate font-mono text-xs">
                          {connector.url}
                        </p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {Object.keys(connector.headers).length} 个请求头 ·
                          值已隐藏
                        </p>
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end gap-1">
                          {controls("connectors", connector)}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                {tab === "actions" &&
                  config.actions.map((action) => (
                    <TableRow key={action.id}>
                      <TableCell>
                        <b className="text-xs font-medium">{action.name}</b>
                        <p className="mt-1 font-mono text-[11px] text-muted-foreground">
                          {action.id}
                        </p>
                      </TableCell>
                      <TableCell>
                        <p className="font-mono text-xs">
                          {action.connector_id}
                        </p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          每批 {action.batch_size} 条 / 等待{" "}
                          {action.batch_wait_ms} ms · 最多 {action.max_attempts}{" "}
                          次尝试
                        </p>
                      </TableCell>
                      <TableCell>
                        <Badge active={action.enabled}>
                          {action.enabled ? "已启用" : "已停用"}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end gap-1">
                          <Button
                            size="sm"
                            variant="ghost"
                            disabled={busy || stopped}
                            onClick={() => void toggle("actions", action.id)}
                          >
                            {action.enabled ? "停用" : "启用"}
                          </Button>
                          {controls("actions", action)}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
            {config[tab].length === 0 && (
              <Empty>
                暂无{names[tab]}。先创建连接器和出站动作，再绑定 SQL 规则。
              </Empty>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-3 border-t px-5 py-3">
            <Button
              size="sm"
              variant="outline"
              disabled={stopped}
              onClick={() => setTest(SQL_EXAMPLE)}
            >
              测试 SQL
            </Button>
            <p className="text-[11px] text-muted-foreground">
              仅计算匹配与输出，不发出 Webhook。
            </p>
          </div>
        </Panel>
      )}
      {status && (
        <Panel
          title="出站动作执行状态"
          extra={
            <span className="text-[10px] text-muted-foreground">
              {sampledAt
                ? `最近采样 ${new Date(sampledAt).toLocaleTimeString()}`
                : "等待采样"}
            </span>
          }
        >
          <div className="overflow-x-auto p-4">
            <Table>
              <TableHeader>
                <TableRow>
                  {[
                    "动作 ID",
                    "排队",
                    "已发送",
                    "失败",
                    "重试",
                    "丢弃",
                    "最近错误",
                  ].map((label) => (
                    <TableHead key={label}>{label}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {status.actions.map((action) => (
                  <TableRow key={action.id}>
                    <TableCell className="font-mono text-xs">
                      {action.id}
                    </TableCell>
                    {[
                      action.queued,
                      action.delivered,
                      action.failed,
                      action.retried,
                      action.dropped,
                    ].map((count, i) => (
                      <TableCell key={i}>{count.toLocaleString()}</TableCell>
                    ))}
                    <TableCell className="max-w-52 truncate text-xs text-muted-foreground">
                      {action.last_error || "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {status.actions.length === 0 && <Empty>暂无动作执行统计</Empty>}
          </div>
          <p className="px-5 pb-4 text-[11px] leading-relaxed text-muted-foreground">
            统计单位为消息条数，重试次数按参与重试的消息计算；服务重启后统计重新开始。
          </p>
        </Panel>
      )}
      {edit && config && (
        <Editor
          key={`${edit.kind}:${edit.fresh ? "new" : edit.value.id}`}
          edit={edit}
          config={config}
          busy={busy}
          stopped={stopped}
          save={saveEdit}
          close={() => !busy && setEdit(null)}
        />
      )}
      {remove && config && (
        <Drawer
          title={`删除${names[remove.kind]}`}
          subtitle={remove.id}
          onClose={() => !busy && setRemove(null)}
        >
          <div className="space-y-4">
            <p className="text-sm">
              确认删除这条配置？保存后将立即应用新的运行版本。
            </p>
            {error && <Notice>{error}</Notice>}
            {deletionError(remove.kind, remove.id, config) && (
              <Notice>{deletionError(remove.kind, remove.id, config)}</Notice>
            )}
            <Button
              variant="destructive"
              disabled={
                busy ||
                stopped ||
                Boolean(deletionError(remove.kind, remove.id, config))
              }
              onClick={() => void confirmRemove()}
            >
              确认删除
            </Button>
            <Button
              className="ml-2"
              variant="outline"
              onClick={() => setRemove(null)}
              disabled={busy}
            >
              取消
            </Button>
          </div>
        </Drawer>
      )}
      {test !== null && (
        <SqlTest
          token={token}
          stopped={stopped}
          sql={test}
          expired={expired}
          close={() => setTest(null)}
        />
      )}
    </div>
  );
}

function Editor({
  edit,
  config,
  busy,
  stopped,
  save,
  close,
}: {
  edit: Editing;
  config: RulesConfig;
  busy: boolean;
  stopped: boolean;
  save: (item: Item) => Promise<void>;
  close: () => void;
}) {
  const [draft, setDraft] = useState(edit.value),
    [error, setError] = useState<string | null>(null);
  const [headers, setHeaders] = useState(() =>
    Object.entries(
      edit.kind === "connectors"
        ? (edit.value as WebhookConnector).headers
        : {},
    ).map(([key, value]) => ({ key, value })),
  );
  function patch(value: Record<string, unknown>) {
    setDraft((old) => ({ ...old, ...value }));
  }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (stopped) return;
    setError(null);
    let next = draft;
    let issue = validateIdentity(draft.id, draft.name);
    if (edit.kind === "rules") {
      const rule = draft as RuleDefinition;
      issue = issue || validateSql(rule.sql);
      if (
        rule.action_ids.some((id) => !config.actions.some((a) => a.id === id))
      )
        issue = "存在无效动作引用";
    } else if (edit.kind === "connectors") {
      if (
        new Set(headers.map((h) => h.key.toLowerCase())).size !== headers.length
      ) {
        setError("请求头名称重复");
        return;
      }
      next = {
        ...draft,
        headers: Object.fromEntries(headers.map((h) => [h.key, h.value])),
      } as WebhookConnector;
      issue = validateConnector(next as WebhookConnector);
    } else issue = validateAction(draft as WebhookAction, config);
    if (issue) {
      setError(issue);
      return;
    }
    try {
      await save(next);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "配置无效");
    }
  }
  const rule = draft as RuleDefinition,
    connector = draft as WebhookConnector,
    action = draft as WebhookAction;
  return (
    <Drawer
      title={`${edit.fresh ? "新建" : "编辑"}${names[edit.kind]}`}
      subtitle="保存后立即生效"
      onClose={close}
    >
      <form className="space-y-5" onSubmit={(event) => void submit(event)}>
        <Field label="ID">
          <Input
            value={draft.id}
            onChange={(e) => patch({ id: e.target.value })}
            disabled={!edit.fresh || busy}
            maxLength={64}
            required
            autoComplete="off"
          />
        </Field>
        <Field label="名称">
          <Input
            value={draft.name}
            onChange={(e) => patch({ name: e.target.value })}
            disabled={busy}
            maxLength={128}
            required
          />
        </Field>
        {edit.kind !== "connectors" && (
          <label className="flex items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={Boolean(rule.enabled)}
              onChange={(e) => patch({ enabled: e.target.checked })}
              disabled={busy}
            />
            启用
          </label>
        )}
        {edit.kind === "rules" && (
          <>
            <Field label="SQL">
              <textarea
                className={textarea}
                value={rule.sql}
                onChange={(e) => patch({ sql: e.target.value })}
                maxLength={8192}
                disabled={busy}
                required
                spellCheck={false}
              />
            </Field>
            <fieldset className="space-y-2">
              <legend className="mb-2 text-xs font-medium">绑定出站动作</legend>
              {config.actions.map((a) => (
                <label key={a.id} className="flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={rule.action_ids.includes(a.id)}
                    disabled={busy}
                    onChange={(e) =>
                      patch({
                        action_ids: e.target.checked
                          ? [...rule.action_ids, a.id]
                          : rule.action_ids.filter((id) => id !== a.id),
                      })
                    }
                  />
                  {a.name}{" "}
                  <span className="font-mono text-muted-foreground">
                    {a.id}
                  </span>
                  {!a.enabled && <span>（已停用）</span>}
                </label>
              ))}
              {config.actions.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  尚无动作。可先保存规则，但不会产生 Webhook。
                </p>
              )}
            </fieldset>
          </>
        )}
        {edit.kind === "connectors" && (
          <>
            <Field label="Webhook URL">
              <Input
                type="url"
                value={connector.url}
                onChange={(e) => patch({ url: e.target.value })}
                disabled={busy}
                required
                placeholder="https://example.com/webhook"
              />
            </Field>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <h3 className="text-xs font-medium">请求头</h3>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={busy || headers.length >= 16}
                  onClick={() =>
                    setHeaders((old) => [...old, { key: "", value: "" }])
                  }
                >
                  添加请求头
                </Button>
              </div>
              {headers.map((header, index) => (
                <div key={index} className="grid gap-2 rounded-md border p-3">
                  <Field label={`请求头名称 ${index + 1}`}>
                    <Input
                      value={header.key}
                      onChange={(e) =>
                        setHeaders((old) =>
                          old.map((h, i) =>
                            i === index ? { ...h, key: e.target.value } : h,
                          ),
                        )
                      }
                      disabled={busy}
                    />
                  </Field>
                  <Field label={`请求头值 ${index + 1}`}>
                    <Input
                      type="password"
                      autoComplete="new-password"
                      value={header.value}
                      onChange={(e) =>
                        setHeaders((old) =>
                          old.map((h, i) =>
                            i === index ? { ...h, value: e.target.value } : h,
                          ),
                        )
                      }
                      disabled={busy}
                    />
                  </Field>
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    disabled={busy}
                    onClick={() =>
                      setHeaders((old) => old.filter((_, i) => i !== index))
                    }
                  >
                    移除此请求头
                  </Button>
                </div>
              ))}
              <p className="text-[11px] leading-relaxed text-muted-foreground">
                已有凭据显示为
                [redacted]，保留该值可继续使用原凭据；输入新值才会替换。请求头值不写入浏览器存储。
              </p>
            </div>
          </>
        )}
        {edit.kind === "actions" && (
          <>
            <Field label="Webhook 连接器">
              <NativeSelect
                value={action.connector_id}
                disabled={busy}
                onChange={(e) => patch({ connector_id: e.target.value })}
                required
              >
                <NativeSelectOption value="">请选择</NativeSelectOption>
                {config.connectors.map((c) => (
                  <NativeSelectOption key={c.id} value={c.id}>
                    {c.name} ({c.id})
                  </NativeSelectOption>
                ))}
              </NativeSelect>
            </Field>
            {(
              [
                ["batch_size", "批量条数", 1, 500],
                ["batch_wait_ms", "批量等待（ms）", 0, 1000],
                ["queue_capacity", "队列容量（条）", 1, 4096],
                ["timeout_ms", "请求超时（ms）", 100, 30000],
                ["max_attempts", "最多尝试次数", 1, 5],
                ["retry_base_ms", "重试基础间隔（ms）", 10, 10000],
              ] as const
            ).map(([key, label, min, max]) => (
              <Field key={key} label={label}>
                <Input
                  type="number"
                  value={Number.isNaN(action[key]) ? "" : action[key]}
                  min={min}
                  max={max}
                  step={1}
                  required
                  disabled={busy}
                  onChange={(e) =>
                    patch({
                      [key]:
                        e.target.value === "" ? NaN : Number(e.target.value),
                    })
                  }
                />
              </Field>
            ))}
            <p className={note}>
              投递失败后将按设置重试。服务中断或待发送消息过多时，部分消息可能无法送达；接收方应避免重复处理。
            </p>
          </>
        )}
        {error && <Notice>{error}</Notice>}
        <div className="flex gap-2 border-t pt-4">
          <Button type="submit" disabled={busy || stopped}>
            {busy ? "正在保存…" : "保存并应用"}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={close}
          >
            取消
          </Button>
        </div>
      </form>
    </Drawer>
  );
}

function SqlTest({
  token,
  stopped,
  sql,
  expired,
  close,
}: {
  token: string;
  stopped: boolean;
  sql: string;
  expired: () => void;
  close: () => void;
}) {
  const [query, setQuery] = useState(sql),
    [topic, setTopic] = useState("devices/test-device/telemetry"),
    [client, setClient] = useState("test-device"),
    [payload, setPayload] = useState('{"temperature":85}');
  const [result, setResult] = useState<{
      matched: boolean;
      output: unknown;
    } | null>(null),
    [error, setError] = useState<string | null>(null),
    [busy, setBusy] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  async function run(event: FormEvent) {
    event.preventDefault();
    if (busy || stopped) return;
    setError(null);
    setResult(null);
    const issue = validateSql(query);
    if (issue) {
      setError(issue);
      return;
    }
    let input;
    try {
      input = testEvent(topic, client, payload);
    } catch (cause) {
      setError((cause as Error).message);
      return;
    }
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    try {
      const next = await api<{ matched: boolean; output: unknown }>(
        "/api/v1/rules/test",
        token,
        controller.signal,
        { method: "POST", body: { sql: query, event: input } },
      );
      if (!controller.signal.aborted) setResult(next);
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (cause instanceof ApiError && cause.status === 401) expired();
        else setError(errorMessage(cause));
      }
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  return (
    <Drawer
      title="测试 SQL"
      subtitle="使用样例预览处理结果，不会发送到 Webhook"
      onClose={close}
    >
      <form className="space-y-4" onSubmit={(event) => void run(event)}>
        <Field label="测试 SQL">
          <textarea
            className={textarea}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            maxLength={8192}
            required
          />
        </Field>
        <Field label="测试 Topic">
          <Input
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
            required
          />
        </Field>
        <Field label="测试 Client ID">
          <Input
            value={client}
            onChange={(e) => setClient(e.target.value)}
            required
          />
        </Field>
        <Field label="测试 Payload">
          <textarea
            className={textarea}
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            maxLength={12000}
            spellCheck={false}
          />
        </Field>
        <Button type="submit" disabled={busy || stopped}>
          {busy ? "正在计算…" : "运行测试"}
        </Button>
        {error && <Notice>{error}</Notice>}
        {result && (
          <div role="status" className="space-y-3">
            <Badge active={result.matched}>
              {result.matched ? "已匹配" : "未匹配"}
            </Badge>
            <pre className="max-h-80 overflow-auto rounded-lg border bg-muted/50 p-3 text-xs whitespace-pre-wrap break-words">
              {JSON.stringify(result.output, null, 2)}
            </pre>
          </div>
        )}
      </form>
    </Drawer>
  );
}
