import { useEffect, useState, type ReactNode } from "react";
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
  backendLabel,
  changeServiceAuth,
  postgresDefaults,
  redisDefaults,
  type AccessDraft,
  type BackendConfig,
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

import { PostgresAccessFields } from "./PostgresAccessFields";
import { RedisAccessFields } from "./RedisAccessFields";

export function AccessControlFields({
  draft,
  onChange,
  busy,
  postgresAvailable = true,
  redisAvailable = true,
}: {
  draft: AccessDraft;
  onChange: (draft: AccessDraft) => void;
  busy: boolean;
  postgresAvailable?: boolean;
  redisAvailable?: boolean;
}) {
  const c = draft.value,
    s = c.service_auth;
  const [headersOpen, setHeadersOpen] = useState(s.type === "headers");
  useEffect(() => {
    if (s.type === "headers") setHeadersOpen(true);
  }, [s.type]);
  const patch = (value: Partial<typeof c>) =>
    onChange({ ...draft, value: { ...c, ...value } });
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
      <Field label="后端类型">
        <SingleSelect
          value={c.type}
          disabled={busy}
          options={[
            { value: "http", label: "HTTP" },
            ...(postgresAvailable
              ? [{ value: "postgresql", label: "PostgreSQL" }]
              : []),
            ...(redisAvailable ? [{ value: "redis", label: "Redis" }] : []),
          ]}
          onValueChange={(value) => {
            const type = value as BackendConfig["type"];
            const stage = "cache_ttl_seconds" in c ? "授权" : "认证";
            onChange({
              ...draft,
              headers: [],
              value: {
                ...c,
                type,
                name: ["HTTP", "PostgreSQL", "Redis"].some(
                  (backend) => c.name === `${backend} ${stage}`,
                )
                  ? `${backendLabel(type)} ${stage}`
                  : c.name,
                url: "",
                service_auth: { type: "none" },
                headers: {},
                allow_http: false,
                postgres:
                  type === "postgresql"
                    ? postgresDefaults("cache_ttl_seconds" in c)
                    : undefined,
                redis: type === "redis" ? redisDefaults() : undefined,
              },
            });
          }}
        />
      </Field>
      {c.type === "postgresql" ? (
        <PostgresAccessFields
          value={c.postgres ?? postgresDefaults("cache_ttl_seconds" in c)}
          authorization={"cache_ttl_seconds" in c}
          busy={busy}
          onChange={(postgres) => patch({ postgres })}
        />
      ) : c.type === "redis" ? (
        <RedisAccessFields
          value={c.redis ?? redisDefaults()}
          authorization={"cache_ttl_seconds" in c}
          busy={busy}
          onChange={(redis) => patch({ redis })}
        />
      ) : (
        <>
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
        </>
      )}
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
        {c.type === "http" && (
          <>
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
          </>
        )}
      </Fold>
    </div>
  );
}
