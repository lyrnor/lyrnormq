import { useEffect, useRef, useState } from "react";
import { api, ApiError, errorMessage } from "./api";
import { Button } from "./components/ui/button";
import {
  ACCESS_PATH,
  draftValue,
  validateAccessDraft,
  type AccessDraft,
  type AccessStage,
  type ProbeResult,
} from "./access-control-model";

export function RedisConnectionTest({
  draft,
  stage,
  token,
  expired,
  disabled,
}: {
  draft: AccessDraft;
  stage: AccessStage;
  token: string;
  expired: () => void;
  disabled: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  async function test() {
    if (request.current) return;
    const invalid = validateAccessDraft(draft, true);
    if (invalid) {
      setMessage(invalid);
      return;
    }
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    setMessage(null);
    try {
      const result = await api<ProbeResult>(
        `${ACCESS_PATH}/test/connection`,
        token,
        controller.signal,
        { method: "POST", body: { config: draftValue(draft), stage } },
      );
      if (!controller.signal.aborted)
        setMessage(
          result.result === "allow"
            ? `Redis 连接成功（${Math.round(result.elapsed_ms)} ms）。`
            : "Redis 连接失败，请检查连接参数、TLS 和网络。",
        );
    } catch (error) {
      if (!controller.signal.aborted) {
        if (error instanceof ApiError && error.status === 401) expired();
        else setMessage(errorMessage(error));
      }
    } finally {
      request.current = null;
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  return (
    <div className="grid gap-2">
      <Button
        type="button"
        variant="outline"
        className="w-fit"
        disabled={busy || disabled}
        onClick={() => void test()}
      >
        {busy ? "正在连接…" : "测试 Redis 连接"}
      </Button>
      {message && (
        <p role="status" className="text-sm">
          {message}
        </p>
      )}
    </div>
  );
}
