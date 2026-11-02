import { useState } from "react";
import { Field } from "./components/ui/field";
import { Input } from "./components/ui/input";
import { Textarea } from "./components/ui/textarea";
import { SingleSelect } from "./components/ui/single-select";
import { Alert, AlertDescription } from "./components/ui/alert";
import {
  changeKafkaProtocol,
  type KafkaConfig,
  type KafkaProtocol,
  type KafkaSecurity,
} from "./kafka-model";
interface Props {
  value: KafkaConfig;
  onChange: (config: KafkaConfig) => void;
  busy: boolean;
}
export function KafkaFields({ value: c, onChange, busy }: Props) {
  const [brokersText, setBrokersText] = useState(() => c.brokers.join("\n"));
  const s = c.security,
    tls = s.protocol === "ssl" || s.protocol === "sasl_ssl",
    sasl = s.protocol.startsWith("sasl_");
  const security = (patch: Partial<KafkaSecurity>) =>
    onChange({ ...c, security: { ...s, ...patch } });
  return (
    <>
      <Field label="Broker 地址">
        <Textarea
          className="min-h-20"
          value={brokersText}
          onChange={(e) => {
            setBrokersText(e.target.value);
            onChange({
              ...c,
              brokers: e.target.value
                .split(/\r?\n|,/)
                .map((v) => v.trim())
                .filter(Boolean),
            });
          }}
          placeholder={"kafka-1.example.com:9092\nkafka-2.example.com:9092"}
          disabled={busy}
          required
          spellCheck={false}
        />
      </Field>
      <p className="-mt-3 text-[11px] text-muted-foreground">
        每行一个地址，支持主机:端口和 [IPv6]:端口，最多 16 个。
      </p>
      <Field label="Kafka Topic">
        <Input
          value={c.topic}
          onChange={(e) => onChange({ ...c, topic: e.target.value })}
          disabled={busy}
          required
          maxLength={249}
          placeholder="mqtt-events"
        />
      </Field>
      <Field label="安全连接">
        <SingleSelect
          aria-label="安全连接"
          value={s.protocol}
          disabled={busy}
          options={[
            { value: "plaintext", label: "不加密 / 无认证" },
            { value: "ssl", label: "TLS" },
            { value: "sasl_plaintext", label: "SASL（不加密）" },
            { value: "sasl_ssl", label: "SASL + TLS" },
          ]}
          onValueChange={(v) =>
            security(changeKafkaProtocol(s, v as KafkaProtocol))
          }
        />
      </Field>
      {sasl && (
        <>
          <Field label="SASL 认证方式">
            <SingleSelect
              aria-label="SASL 认证方式"
              value={s.sasl_mechanism ?? "SCRAM-SHA-256"}
              disabled={busy}
              options={[
                { value: "PLAIN", label: "PLAIN" },
                { value: "SCRAM-SHA-256", label: "SCRAM-SHA-256" },
                { value: "SCRAM-SHA-512", label: "SCRAM-SHA-512" },
              ]}
              onValueChange={(v) =>
                security({
                  sasl_mechanism: v as KafkaSecurity["sasl_mechanism"],
                })
              }
            />
          </Field>
          <Field label="用户名">
            <Input
              value={s.username ?? ""}
              onChange={(e) => security({ username: e.target.value || null })}
              disabled={busy}
              required
              autoComplete="off"
            />
          </Field>
          <Field label="密码">
            <Input
              type="password"
              value={s.password ?? ""}
              onChange={(e) => security({ password: e.target.value || null })}
              disabled={busy}
              required
              autoComplete="new-password"
            />
          </Field>
          <p className="-mt-3 text-[11px] text-muted-foreground">
            已有密码已隐藏；保持原值继续使用，输入新值后替换。
          </p>
        </>
      )}
      {tls && (
        <div className="space-y-4 rounded-lg border p-4">
          <p className="text-xs font-medium">TLS 证书</p>
          <Field label="CA 证书（可选）">
            <Textarea
              className="min-h-20 font-mono text-xs"
              value={s.ca_pem ?? ""}
              onChange={(e) => security({ ca_pem: e.target.value || null })}
              disabled={busy}
              placeholder="-----BEGIN CERTIFICATE-----"
              spellCheck={false}
            />
          </Field>
          <p className="text-[11px] text-muted-foreground">
            留空使用系统信任的证书。服务器证书和主机名始终会校验。
          </p>
          <Field label="客户端证书（可选）">
            <Textarea
              className="min-h-20 font-mono text-xs"
              value={s.certificate_pem ?? ""}
              onChange={(e) =>
                security({ certificate_pem: e.target.value || null })
              }
              disabled={busy}
              placeholder="-----BEGIN CERTIFICATE-----"
              spellCheck={false}
            />
          </Field>
          <Field label="客户端私钥（可选）">
            <Textarea
              className="min-h-20 font-mono text-xs"
              value={s.private_key_pem ?? ""}
              onChange={(e) =>
                security({
                  private_key_pem: e.target.value || null,
                  ...(e.target.value ? {} : { private_key_password: null }),
                })
              }
              disabled={busy}
              placeholder="-----BEGIN PRIVATE KEY-----"
              spellCheck={false}
            />
          </Field>
          <Field label="私钥口令（可选）">
            <Input
              type="password"
              value={s.private_key_password ?? ""}
              onChange={(e) =>
                security({ private_key_password: e.target.value || null })
              }
              disabled={busy}
              autoComplete="new-password"
            />
          </Field>
          <p className="text-[11px] text-muted-foreground">
            仅双向认证时填写客户端证书和私钥，两者需成对提供。已有私钥和口令已隐藏；保持原值继续使用。
          </p>
        </div>
      )}
    </>
  );
}
export function KafkaAdvancedFields({ value: c, onChange, busy }: Props) {
  const number = (
    key: "linger_ms" | "batch_bytes",
    value: string,
    factor = 1,
  ) => onChange({ ...c, [key]: value === "" ? NaN : Number(value) * factor });
  return (
    <>
      <Field label="Key 字段（可选）">
        <Input
          value={c.key_field ?? ""}
          onChange={(e) =>
            onChange({ ...c, key_field: e.target.value || null })
          }
          disabled={busy}
          placeholder="留空使用 Client ID"
        />
      </Field>
      <Alert role="note">
        <AlertDescription>
          默认使用 Client
          ID，将同一客户端的消息发送到相同分区。自定义字段必须来自规则输出且为字符串；字段值或分区数量变化时，不能保证跨分区的消费顺序。
        </AlertDescription>
      </Alert>
      <Field label="压缩方式">
        <SingleSelect
          aria-label="压缩方式"
          value={c.compression}
          disabled={busy}
          options={[
            { value: "none", label: "不压缩" },
            { value: "lz4", label: "LZ4" },
            { value: "zstd", label: "Zstd" },
          ]}
          onValueChange={(v) =>
            onChange({ ...c, compression: v as KafkaConfig["compression"] })
          }
        />
      </Field>
      <Field label="发送等待（ms）">
        <Input
          type="number"
          min={0}
          max={100}
          step={1}
          required
          value={Number.isNaN(c.linger_ms) ? "" : c.linger_ms}
          onChange={(e) => number("linger_ms", e.target.value)}
          disabled={busy}
        />
      </Field>
      <Field label="发送批次大小（KiB）">
        <Input
          type="number"
          min={16}
          max={1024}
          step="any"
          required
          value={Number.isNaN(c.batch_bytes) ? "" : c.batch_bytes / 1024}
          onChange={(e) => number("batch_bytes", e.target.value, 1024)}
          disabled={busy}
        />
      </Field>
      <p className="-mt-3 text-[11px] text-muted-foreground">
        较短等待降低延迟，较大批次可提高吞吐；每条消息仍独立投递。
      </p>
    </>
  );
}
