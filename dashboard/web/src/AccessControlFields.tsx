import { useEffect, useRef, useState, type ReactNode } from "react";
import { ChevronDown, Plus, Trash2 } from "lucide-react";
import { Alert, AlertDescription } from "./components/ui/alert";
import { Button } from "./components/ui/button";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./components/ui/collapsible";
import { Field } from "./components/ui/field";
import { Input } from "./components/ui/input";
import { SingleSelect } from "./components/ui/single-select";
import { Switch } from "./components/ui/switch";
import {
  changeServiceAuth,
  utf8Bytes,
  type AccessDraft,
  type ServiceAuth,
} from "./access-control-model";

function Fold({
  title,
  children,
  open,
  onOpenChange,
}: {
  title: string;
  children: ReactNode;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  return (
    <Collapsible
      open={open}
      onOpenChange={onOpenChange}
      className="rounded-lg border"
    >
      <CollapsibleTrigger className="flex w-full items-center justify-between px-4 py-3 text-sm font-medium [&[data-state=open]>svg]:rotate-180">
        {title}
        <ChevronDown className="size-4 transition-transform" />
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="grid gap-4 border-t p-4">{children}</div>
      </CollapsibleContent>
    </Collapsible>
  );
}

function PemFile({
  label,
  value,
  secret = false,
  disabled,
  onChange,
  onReadingChange,
}: {
  label: string;
  value: string | null;
  secret?: boolean;
  disabled: boolean;
  onChange: (value: string | null) => void;
  onReadingChange: (reading: boolean) => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [reading, setReading] = useState(false);
  const epoch = useRef(0),
    readingRef = useRef(false),
    readingCallback = useRef(onReadingChange);
  readingCallback.current = onReadingChange;
  const markReading = (value: boolean) => {
    readingRef.current = value;
    setReading(value);
    readingCallback.current(value);
  };
  useEffect(
    () => () => {
      epoch.current++;
      if (readingRef.current) readingCallback.current(false);
    },
    [],
  );
  return (
    <div className="grid gap-2">
      <Field label={label}>
        <Input
          type="file"
          accept=".pem,.crt,.cer,.key"
          disabled={disabled || reading}
          onChange={(e) => {
            const file = e.target.files?.[0];
            e.target.value = "";
            const current = ++epoch.current;
            setError(null);
            if (!file) return;
            if (file.size > 16384) {
              setError("每个文件最多 16 KiB。");
              return;
            }
            markReading(true);
            void file
              .text()
              .then((text) => {
                if (epoch.current !== current) return;
                if (utf8Bytes(text) > 16384 || !text.includes("-----BEGIN ")) {
                  setError("请选择有效的 PEM 文件。");
                  return;
                }
                if (secret && text.includes("ENCRYPTED")) {
                  setError("目前仅支持未加密的客户端私钥。");
                  return;
                }
                onChange(text);
              })
              .catch(() => {
                if (epoch.current === current)
                  setError("无法读取文件，请重新选择。");
              })
              .finally(() => {
                if (epoch.current === current) markReading(false);
              });
          }}
        />
      </Field>
      <div className="flex items-center gap-3 text-xs text-muted-foreground">
        <span>
          {reading ? "正在读取…" : value ? "已配置" : "未配置（可选）"}
        </span>
        {value && (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            disabled={disabled}
            onClick={() => {
              epoch.current++;
              markReading(false);
              setError(null);
              onChange(null);
            }}
          >
            移除{label}
          </Button>
        )}
      </div>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
    </div>
  );
}

export function AccessControlFields({
  draft,
  onChange,
  busy,
  onReadingChange,
}: {
  draft: AccessDraft;
  onChange: (draft: AccessDraft) => void;
  busy: boolean;
  onReadingChange: (reading: boolean) => void;
}) {
  const c = draft.value,
    s = c.service_auth;
  const readingFiles = useRef(new Set<string>());
  const fileReading = (label: string, reading: boolean) => {
    if (reading) readingFiles.current.add(label);
    else readingFiles.current.delete(label);
    onReadingChange(readingFiles.current.size > 0);
  };
  const [headersOpen, setHeadersOpen] = useState(s.type === "headers");
  useEffect(() => {
    if (s.type === "headers") setHeadersOpen(true);
  }, [s.type]);
  const patch = (value: Partial<typeof c>) =>
    onChange({ ...draft, value: { ...c, ...value } });
  const tls = (value: Partial<typeof c.tls>) =>
    patch({ tls: { ...c.tls, ...value } });
  return (
    <div className="grid gap-5">
      <Field label="名称">
        <Input
          value={c.name}
          maxLength={128}
          disabled={busy}
          onChange={(e) => patch({ name: e.target.value })}
          required
        />
      </Field>
      <Field label="请求地址">
        <Input
          type="url"
          value={c.url}
          maxLength={2048}
          placeholder="https://example.com/mqtt/authenticate"
          disabled={busy}
          onChange={(e) => patch({ url: e.target.value })}
          autoComplete="off"
          spellCheck={false}
        />
      </Field>
      <Field label="服务鉴权">
        <SingleSelect
          value={s.type}
          disabled={busy}
          options={[
            { value: "none", label: "无" },
            { value: "bearer", label: "Bearer Token" },
            { value: "basic", label: "Basic Auth" },
            { value: "headers", label: "自定义请求头" },
          ]}
          onValueChange={(value) =>
            patch({
              service_auth: changeServiceAuth(value as ServiceAuth["type"]),
            })
          }
        />
      </Field>
      {s.type === "bearer" && (
        <Field label="服务 Token">
          <Input
            type="password"
            value={s.token}
            disabled={busy}
            maxLength={1024}
            autoComplete="off"
            onChange={(e) =>
              patch({ service_auth: { ...s, token: e.target.value } })
            }
          />
        </Field>
      )}
      {s.type === "basic" && (
        <>
          <Field label="服务用户名">
            <Input
              value={s.username}
              disabled={busy}
              maxLength={256}
              autoComplete="off"
              onChange={(e) =>
                patch({ service_auth: { ...s, username: e.target.value } })
              }
            />
          </Field>
          <Field label="服务密码">
            <Input
              type="password"
              value={s.password}
              disabled={busy}
              maxLength={1024}
              autoComplete="new-password"
              onChange={(e) =>
                patch({ service_auth: { ...s, password: e.target.value } })
              }
            />
          </Field>
        </>
      )}
      {(s.type === "bearer" || s.type === "basic") && (
        <p className="-mt-3 text-xs text-muted-foreground">
          这是 Broker
          访问服务的凭据，与设备登录密码分开。已有凭据已隐藏，输入新值即可替换。
        </p>
      )}
      <Fold title="请求头" open={headersOpen} onOpenChange={setHeadersOpen}>
        {draft.headers.map((header, index) => (
          <div key={index} className="grid grid-cols-[1fr_1fr_auto] gap-2">
            <Input
              aria-label={`请求头名称 ${index + 1}`}
              placeholder="名称"
              value={header.name}
              disabled={busy}
              onChange={(e) =>
                onChange({
                  ...draft,
                  headers: draft.headers.map((v, i) =>
                    i === index ? { ...v, name: e.target.value } : v,
                  ),
                })
              }
              spellCheck={false}
            />
            <Input
              aria-label={`请求头值 ${index + 1}`}
              type="password"
              placeholder="值"
              value={header.value}
              disabled={busy}
              autoComplete="off"
              onChange={(e) =>
                onChange({
                  ...draft,
                  headers: draft.headers.map((v, i) =>
                    i === index ? { ...v, value: e.target.value } : v,
                  ),
                })
              }
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              aria-label={`移除请求头 ${index + 1}`}
              disabled={busy}
              onClick={() =>
                onChange({
                  ...draft,
                  headers: draft.headers.filter((_, i) => i !== index),
                })
              }
            >
              <Trash2 className="size-4" />
            </Button>
          </div>
        ))}
        <Button
          type="button"
          variant="outline"
          className="w-fit"
          disabled={busy || draft.headers.length >= 32}
          onClick={() =>
            onChange({
              ...draft,
              headers: [...draft.headers, { name: "", value: "" }],
            })
          }
        >
          <Plus className="size-4" />
          添加请求头
        </Button>
      </Fold>
      <Fold title="高级设置">
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="请求超时（ms）">
            <Input
              type="number"
              min={100}
              max={5000}
              step={1}
              value={c.timeout_ms}
              disabled={busy}
              onChange={(e) => patch({ timeout_ms: e.target.valueAsNumber })}
            />
          </Field>
          <Field label="最大并发">
            <Input
              type="number"
              min={1}
              max={256}
              step={1}
              value={c.max_concurrency}
              disabled={busy}
              onChange={(e) =>
                patch({ max_concurrency: e.target.valueAsNumber })
              }
            />
          </Field>
        </div>
        {"cache_ttl_seconds" in c && (
          <>
            <Field label="决策缓存时间（秒）">
              <Input
                type="number"
                min={0}
                max={300}
                step={1}
                value={c.cache_ttl_seconds}
                disabled={busy}
                onChange={(e) =>
                  patch({ cache_ttl_seconds: e.target.valueAsNumber })
                }
              />
            </Field>
            <p className="-mt-2 text-xs text-muted-foreground">
              0 表示每次查询。缓存有效期内可能沿用之前的允许或拒绝结果。
            </p>
          </>
        )}
        <Field label="允许明文 HTTP">
          <Switch
            checked={c.allow_http}
            disabled={busy}
            onCheckedChange={(checked) => patch({ allow_http: checked })}
          />
        </Field>
        {c.allow_http && (
          <Alert>
            <AlertDescription>
              明文连接无法保护传输中的设备密码和服务凭据，请仅在可信网络中使用。
            </AlertDescription>
          </Alert>
        )}
        <div className="grid gap-4 border-t pt-4">
          <p className="text-sm font-medium">HTTPS 证书（可选）</p>
          <PemFile
            onReadingChange={(reading) => fileReading("ca", reading)}
            label="CA 证书"
            value={c.tls.ca_pem}
            disabled={busy}
            onChange={(value) => tls({ ca_pem: value })}
          />
          <PemFile
            onReadingChange={(reading) => fileReading("certificate", reading)}
            label="客户端证书"
            value={c.tls.certificate_pem}
            disabled={busy}
            onChange={(value) => tls({ certificate_pem: value })}
          />
          <PemFile
            onReadingChange={(reading) => fileReading("key", reading)}
            label="客户端私钥"
            value={c.tls.private_key_pem}
            secret
            disabled={busy}
            onChange={(value) => tls({ private_key_pem: value })}
          />
          <p className="text-xs text-muted-foreground">
            上传 PEM 文件。客户端证书与未加密私钥需成对提供。
          </p>
        </div>
      </Fold>
    </div>
  );
}
