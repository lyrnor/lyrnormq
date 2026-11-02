import { useEffect, useRef, useState, type FormEvent } from "react";
import { api, ApiError, errorMessage } from "./api";
import { Drawer } from "./components";
import { Alert, AlertDescription } from "./components/ui/alert";
import { Button } from "./components/ui/button";
import { Field } from "./components/ui/field";
import { Input } from "./components/ui/input";
import { SingleSelect } from "./components/ui/single-select";
import {
  ACCESS_PATH,
  draftValue,
  utf8Bytes,
  type AccessStage,
  type AccessDraft,
  type ProbeResult,
} from "./access-control-model";

export function AccessControlTest({
  stage,
  draft,
  token,
  expired,
  onClose,
}: {
  stage: AccessStage;
  draft: AccessDraft;
  token: string;
  expired: () => void;
  onClose: () => void;
}) {
  const [client, setClient] = useState(""),
    [username, setUsername] = useState(""),
    [password, setPassword] = useState("");
  const [action, setAction] = useState("publish"),
    [topic, setTopic] = useState("");
  const [busy, setBusy] = useState(false),
    [error, setError] = useState<string | null>(null),
    [result, setResult] = useState<ProbeResult | null>(null);
  const request = useRef<AbortController | null>(null),
    pending = useRef(false);
  useEffect(
    () => () => {
      request.current?.abort();
    },
    [],
  );
  async function submit(e: FormEvent) {
    e.preventDefault();
    if (pending.current) return;
    if (
      !client ||
      utf8Bytes(client) > 65535 ||
      !username ||
      utf8Bytes(username) > 256 ||
      /[\u0000-\u001f\u007f-\u009f]/.test(username)
    ) {
      setError("请填写有效的 Client ID 和设备 Username。");
      return;
    }
    if (stage === "authentication" && utf8Bytes(password) > 65535) {
      setError("设备密码过长。");
      return;
    }
    if (
      stage === "authorization" &&
      (!topic ||
        utf8Bytes(topic) > 65535 ||
        topic.includes("\0") ||
        (action === "publish" && /[+#]/.test(topic)))
    ) {
      setError("请填写有效的 Topic；发布 Topic 不能包含通配符。");
      return;
    }
    pending.current = true;
    setBusy(true);
    setError(null);
    setResult(null);
    const controller = new AbortController();
    request.current = controller;
    try {
      const value = await api<ProbeResult>(
        `${ACCESS_PATH}/test/${stage}`,
        token,
        controller.signal,
        {
          method: "POST",
          body: {
            config: draftValue(draft),
            client_id: client,
            username,
            ...(stage === "authentication" ? { password } : { action, topic }),
          },
        },
      );
      if (!controller.signal.aborted) {
        setResult(value);
        setPassword("");
      }
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (cause instanceof ApiError && cause.status === 401) expired();
        else setError(errorMessage(cause));
      }
    } finally {
      pending.current = false;
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  const label = stage === "authentication" ? "认证" : "授权";
  return (
    <Drawer
      title={`测试${label}`}
      subtitle="使用当前填写的配置，不保存配置，也不改变设备连接。"
      onClose={onClose}
    >
      <form className="grid gap-4" onSubmit={(e) => void submit(e)}>
        <Field label="Client ID">
          <Input
            value={client}
            onChange={(e) => setClient(e.target.value)}
            autoComplete="off"
            disabled={busy}
            required
          />
        </Field>
        <Field label="设备 Username">
          <Input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="off"
            disabled={busy}
            required
          />
        </Field>
        {stage === "authentication" ? (
          <Field label="设备 Password">
            <Input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="new-password"
              disabled={busy}
            />
          </Field>
        ) : (
          <>
            <Field label="操作类型">
              <SingleSelect
                value={action}
                disabled={busy}
                options={[
                  { value: "publish", label: "发布" },
                  { value: "subscribe", label: "订阅" },
                ]}
                onValueChange={setAction}
              />
            </Field>
            <Field label="Topic">
              <Input
                value={topic}
                onChange={(e) => setTopic(e.target.value)}
                autoComplete="off"
                spellCheck={false}
                disabled={busy}
                required
              />
            </Field>
          </>
        )}
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        {result && (
          <Alert
            variant={
              result.result === "unavailable" ? "destructive" : "default"
            }
            role="status"
          >
            <AlertDescription>
              <strong>
                {result.result === "unavailable"
                  ? draft.value.type === "postgresql"
                    ? "数据库异常，请检查连接、SQL 和返回列。"
                    : "服务异常，请检查地址、网络和服务鉴权。"
                  : result.result === "deny"
                    ? `${label === "认证" ? "认证失败" : "拒绝当前操作"}`
                    : `${label === "认证" ? "认证通过" : "允许当前操作"}`}
              </strong>
              <p className="mt-1">
                {result.http_status !== null
                  ? `HTTP ${result.http_status} · `
                  : ""}
                {Math.round(result.elapsed_ms)} ms
              </p>
            </AlertDescription>
          </Alert>
        )}
        <Button type="submit" disabled={busy}>
          {busy ? "正在测试…" : "开始测试"}
        </Button>
      </form>
    </Drawer>
  );
}
