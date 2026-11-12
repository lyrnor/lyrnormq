import { Field } from "./components/ui/field";
import { Input } from "./components/ui/input";
import { SingleSelect } from "./components/ui/single-select";
import type { RedisConfig } from "./access-control-model";

export function RedisAccessFields({
  value: config,
  authorization,
  busy,
  onChange,
}: {
  value: RedisConfig;
  authorization: boolean;
  busy: boolean;
  onChange: (value: RedisConfig) => void;
}) {
  const patch = (value: Partial<RedisConfig>) =>
    onChange({ ...config, ...value });
  return (
    <div className="grid gap-4">
      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Redis 主机">
          <Input
            value={config.host}
            disabled={busy}
            onChange={(e) => patch({ host: e.target.value })}
            autoComplete="off"
          />
        </Field>
        <Field label="Redis 端口">
          <Input
            type="number"
            min={1}
            max={65535}
            value={config.port}
            disabled={busy}
            onChange={(e) => patch({ port: e.target.valueAsNumber })}
          />
        </Field>
        <Field label="Redis 用户名">
          <Input
            value={config.username}
            disabled={busy}
            onChange={(e) => patch({ username: e.target.value })}
            autoComplete="off"
            placeholder="可留空"
          />
        </Field>
        <Field label="键前缀">
          <Input
            value={config.key_prefix}
            disabled={busy}
            onChange={(e) => patch({ key_prefix: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
        </Field>
      </div>
      <Field label="Redis 密码">
        <Input
          type="password"
          value={config.password}
          maxLength={1024}
          disabled={busy}
          onChange={(e) => patch({ password: e.target.value })}
          autoComplete="new-password"
        />
      </Field>
      <p className="text-xs text-muted-foreground">
        已有密码已隐藏，保留原值即可继续使用；输入新值替换。这是 Broker
        连接 Redis 的密码，可以留空。
      </p>
      <Field label="Redis TLS">
        <SingleSelect
          value={config.tls_mode}
          disabled={busy}
          options={[
            { value: "verify_full", label: "TLS：验证证书和主机名" },
            { value: "disable", label: "明文：仅可信网络" },
          ]}
          onValueChange={(value) =>
            patch({ tls_mode: value as RedisConfig["tls_mode"] })
          }
        />
      </Field>
      {config.tls_mode === "verify_full" && (
        <Field label="自定义 CA 证书（PEM，可选）">
          <textarea
            className="min-h-24 w-full rounded-md border p-3 font-mono text-xs"
            value={config.ca_pem ?? ""}
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
          value={config.max_connections}
          disabled={busy}
          onChange={(e) => patch({ max_connections: e.target.valueAsNumber })}
        />
      </Field>
      <p className="text-xs leading-relaxed text-muted-foreground">
        {authorization
          ? "授权读取 HASH 字段 publish 或 subscribe。值为 JSON 数组，每项包含 effect（allow/deny）、topic_filter 和整数 priority。指定 Client ID 的规则放在字段名 publish 或 subscribe 后加 ASCII 0x1F 再加 Client ID。优先级高者优先，同级拒绝优先；无匹配则拒绝，合并后最多 256 条。"
          : "认证读取 HASH 字段 password_hash，值为 Argon2id PHC 字符串。无记录或密码不匹配则拒绝。"}
      </p>
      <p className="text-xs text-muted-foreground">
        键为{" "}
        <span className="font-mono">
          {config.key_prefix || "lyrnormq:"}
          {authorization ? "acl:" : "auth:"}
          {"{username}"}
        </span>
        。逻辑库固定为 0。Broker 只读，不创建这些键。
      </p>
    </div>
  );
}
