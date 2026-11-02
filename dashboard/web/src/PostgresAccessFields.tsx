import { Field } from "./components/ui/field";
import { Input } from "./components/ui/input";
import { SingleSelect } from "./components/ui/single-select";
import type { PostgresConfig } from "./access-control-model";

export function PostgresAccessFields({
  value: p,
  authorization,
  busy,
  onChange,
}: {
  value: PostgresConfig;
  authorization: boolean;
  busy: boolean;
  onChange: (value: PostgresConfig) => void;
}) {
  const patch = (value: Partial<PostgresConfig>) =>
    onChange({ ...p, ...value });
  return (
    <div className="grid gap-4">
      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="数据库主机">
          <Input
            value={p.host}
            disabled={busy}
            onChange={(e) => patch({ host: e.target.value })}
            autoComplete="off"
          />
        </Field>
        <Field label="数据库端口">
          <Input
            type="number"
            min={1}
            max={65535}
            value={p.port}
            disabled={busy}
            onChange={(e) => patch({ port: e.target.valueAsNumber })}
          />
        </Field>
        <Field label="数据库名称">
          <Input
            value={p.database}
            disabled={busy}
            onChange={(e) => patch({ database: e.target.value })}
          />
        </Field>
        <Field label="数据库用户名">
          <Input
            value={p.username}
            disabled={busy}
            onChange={(e) => patch({ username: e.target.value })}
            autoComplete="off"
          />
        </Field>
      </div>
      <Field label="数据库密码">
        <Input
          type="password"
          value={p.password}
          maxLength={1024}
          disabled={busy}
          onChange={(e) => patch({ password: e.target.value })}
          autoComplete="new-password"
        />
      </Field>
      <p className="text-xs text-muted-foreground">
        已有密码已隐藏，保留原值即可继续使用；输入新值替换。这是 Broker
        连接数据库的密码。
      </p>
      <Field label="数据库 TLS">
        <SingleSelect
          value={p.tls_mode}
          disabled={busy}
          options={[
            { value: "verify_full", label: "TLS：验证证书和主机名" },
            { value: "disable", label: "明文：仅可信网络" },
          ]}
          onValueChange={(value) =>
            patch({ tls_mode: value as PostgresConfig["tls_mode"] })
          }
        />
      </Field>
      {p.tls_mode === "verify_full" && (
        <Field label="自定义 CA 证书（PEM，可选）">
          <textarea
            className="min-h-24 w-full rounded-md border p-3 font-mono text-xs"
            value={p.ca_pem ?? ""}
            disabled={busy}
            onChange={(e) => patch({ ca_pem: e.target.value || null })}
            spellCheck={false}
          />
        </Field>
      )}
      <Field label="连接池上限">
        <Input
          type="number"
          min={1}
          max={32}
          value={p.max_connections}
          disabled={busy}
          onChange={(e) => patch({ max_connections: e.target.valueAsNumber })}
        />
      </Field>
      <Field label="查询 SQL">
        <textarea
          className="min-h-40 w-full rounded-md border p-3 font-mono text-xs"
          value={p.query}
          disabled={busy}
          onChange={(e) => patch({ query: e.target.value })}
          spellCheck={false}
        />
      </Field>
      <p className="text-xs leading-relaxed text-muted-foreground">
        {authorization
          ? "$1 = 用户名，$2 = Client ID，$3 = publish 或 subscribe。返回 effect（allow/deny）、topic_filter 和整数 priority。优先级高者优先，同级拒绝优先；无匹配则拒绝，最多 256 条规则。"
          : "$1 = 用户名。返回唯一一行 password_hash（Argon2id PHC 字符串）；可用 AS password_hash 适配已有字段。无记录或密码不匹配则拒绝。"}
      </p>
      <p className="text-xs text-muted-foreground">
        表名和查询内容由你配置，Broker
        不创建业务表。请使用仅有必要读取权限的数据库账号。
      </p>
    </div>
  );
}
