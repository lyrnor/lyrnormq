import type { Overview, Query, Rate, Route } from "./types";
export const routes: Route[] = [
  "overview",
  "connections",
  "topics",
  "security",
  "listeners",
  "configuration",
  "logs",
  "rules",
];
export function routeFromHash(hash: string): Route {
  const value = hash.replace(/^#/, "");
  return routes.includes(value as Route) ? (value as Route) : "overview";
}
export const initialQuery: Query = {
  offset: 0,
  limit: 25,
  search: "",
  state: "",
  protocol: "",
  node: "",
};
export function changeQuery(query: Query, patch: Partial<Query>): Query {
  return { ...query, ...patch, offset: patch.offset ?? 0 };
}
export function queryString(query: Query): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query))
    if (value !== "") params.set(key, String(value));
  return params.toString();
}
export function pageBounds(total: number, offset: number, limit: number) {
  return {
    start: total && offset < total ? offset + 1 : 0,
    end: offset < total ? Math.min(offset + limit, total) : 0,
    previous: offset > 0,
    next: offset + limit < total && offset + limit <= 1_000_000,
  };
}
export function protocolLabel(protocol: string): string {
  return protocol === "unknown" ? "未协商" : `MQTT ${protocol}`;
}
export function boundedSearch(value: string): string {
  let output = "";
  for (const char of value) {
    if (new TextEncoder().encode(output + char).length > 256) break;
    output += char;
  }
  return output;
}
export function addRate(
  previous: Overview | null,
  next: Overview,
  history: Rate[],
): Rate[] {
  if (!previous) return [];
  const elapsed = next.sampled_at_millis - previous.sampled_at_millis;
  const incoming = next.messages_in_total - previous.messages_in_total;
  const outgoing = next.messages_out_total - previous.messages_out_total;
  if (
    elapsed <= 0 ||
    next.uptime_seconds < previous.uptime_seconds ||
    incoming < 0 ||
    outgoing < 0 ||
    !Number.isSafeInteger(next.messages_in_total) ||
    !Number.isSafeInteger(next.messages_out_total)
  )
    return [];
  return [
    ...history,
    {
      at: next.sampled_at_millis,
      incoming: (incoming * 1000) / elapsed,
      outgoing: (outgoing * 1000) / elapsed,
    },
  ].slice(-60);
}
export function bytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1048576) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1048576).toFixed(2)} MiB`;
}
export function duration(seconds: number): string {
  const days = Math.floor(seconds / 86400),
    hours = Math.floor((seconds % 86400) / 3600),
    minutes = Math.floor((seconds % 3600) / 60);
  return `${days ? `${days} 天 ` : ""}${hours} 小时 ${minutes} 分钟`;
}
export type AuthState = { token: string | null; error: string | null };
export function authReducer(
  _state: AuthState,
  action:
    { type: "login"; token: string } | { type: "logout" } | { type: "expired" },
): AuthState {
  if (action.type === "login") return { token: action.token, error: null };
  return {
    token: null,
    error: action.type === "expired" ? "管理令牌已失效，请重新登录。" : null,
  };
}
