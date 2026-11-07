import { Badge, Panel } from "./components";
import type { Topology, TopologyNode } from "./types";

function shortAddress(address: string | null | undefined): string {
  if (!address) return "—";
  try {
    const url = new URL(address.includes("://") ? address : `tcp://${address}`);
    return url.host || address;
  } catch {
    return address.length > 36 ? `${address.slice(0, 34)}…` : address;
  }
}

function roleLabel(role: string): string {
  if (role === "controller") return "Controller";
  if (role === "broker") return "Broker";
  return role;
}

function roleSummary(node: TopologyNode): string {
  if (node.roles.includes("controller") && node.roles.includes("broker")) {
    return "Controller · Broker";
  }
  return node.roles.map(roleLabel).join(" · ") || "Unknown";
}

function nodeTone(node: TopologyNode): string {
  if (node.excluded) return "border-orange-200 bg-orange-50 text-orange-900";
  if (!node.online) return "border-slate-200 bg-slate-50 text-slate-600";
  if (node.leader) return "border-emerald-200 bg-emerald-50 text-emerald-950";
  return "border-slate-200 bg-white text-slate-800";
}

function healthLabel(node: TopologyNode): string {
  if (node.excluded) return "已排除";
  if (!node.online) return "租约过期";
  return "健康";
}

function leaseLabel(ms: number): string {
  if (!Number.isFinite(ms)) return "—";
  if (ms < 0) return `过期 ${Math.abs(Math.round(ms / 100)) / 10}s`;
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

type Placed = { node: TopologyNode; x: number; y: number };
type EdgeKind = "raft" | "replica" | "metadata";

function placeTriangle(
  row: TopologyNode[],
  apexY: number,
  baseY: number,
): Placed[] {
  if (row.length === 0) return [];
  if (row.length === 1) return [{ node: row[0], x: 340, y: (apexY + baseY) / 2 }];
  if (row.length === 2) {
    return [
      { node: row[0], x: 240, y: (apexY + baseY) / 2 },
      { node: row[1], x: 440, y: (apexY + baseY) / 2 },
    ];
  }
  if (row.length === 3) {
    // Explicit triangle so all three mesh edges stay visible.
    return [
      { node: row[0], x: 180, y: baseY },
      { node: row[1], x: 340, y: apexY },
      { node: row[2], x: 500, y: baseY },
    ];
  }
  const span = Math.min(520, 140 * (row.length - 1));
  const start = 340 - span / 2;
  return row.map((node, index) => ({
    node,
    x: start + (span * index) / (row.length - 1),
    y: baseY,
  }));
}

function layout(nodes: TopologyNode[]): Placed[] {
  const controllers = nodes.filter((n) => n.roles.includes("controller"));
  const brokers = nodes.filter((n) => n.roles.includes("broker"));
  // Split planes when both roles exist; otherwise keep a circle.
  if (controllers.length > 0 && brokers.length > 0) {
    return [
      ...placeTriangle(controllers, 72, 150),
      ...placeTriangle(brokers, 360, 290),
    ];
  }
  const count = Math.max(nodes.length, 1);
  const radius = count === 1 ? 0 : count === 2 ? 150 : count === 3 ? 168 : 180;
  return nodes.map((node, index) => {
    const angle = -Math.PI / 2 + (index * (2 * Math.PI)) / count;
    return {
      node,
      x: 340 + radius * Math.cos(angle),
      y: 210 + radius * Math.sin(angle),
    };
  });
}

/** Real cluster edges: Raft mesh, replica mesh, brokers → Raft leader. */
function edges(
  placed: Placed[],
  leaderId: number | null,
): Array<{ from: Placed; to: Placed; kind: EdgeKind }> {
  const result: Array<{ from: Placed; to: Placed; kind: EdgeKind }> = [];
  const controllers = placed.filter((p) => p.node.roles.includes("controller"));
  const brokers = placed.filter((p) => p.node.roles.includes("broker"));
  for (let i = 0; i < controllers.length; i += 1) {
    for (let j = i + 1; j < controllers.length; j += 1) {
      result.push({ from: controllers[i], to: controllers[j], kind: "raft" });
    }
  }
  for (let i = 0; i < brokers.length; i += 1) {
    for (let j = i + 1; j < brokers.length; j += 1) {
      result.push({ from: brokers[i], to: brokers[j], kind: "replica" });
    }
  }
  const leader =
    controllers.find((p) => p.node.leader) ??
    controllers.find((p) => p.node.id === leaderId) ??
    null;
  if (leader) {
    for (const broker of brokers) {
      result.push({ from: broker, to: leader, kind: "metadata" });
    }
  }
  return result;
}

function edgeStroke(kind: EdgeKind): { stroke: string; dash?: string; width: number } {
  if (kind === "raft") return { stroke: "#8b5cf6", width: 2 };
  if (kind === "replica") return { stroke: "#38bdf8", width: 1.75 };
  return { stroke: "#34d399", dash: "5 5", width: 1.5 };
}

function RoleChip({ role }: { role: string }) {
  const tone =
    role === "controller"
      ? "border-violet-200 bg-violet-50 text-violet-800"
      : role === "broker"
        ? "border-sky-200 bg-sky-50 text-sky-800"
        : "border-slate-200 bg-slate-50 text-slate-700";
  return (
    <span
      className={`rounded border px-1.5 py-0.5 text-[9px] font-semibold tracking-wide ${tone}`}
    >
      {roleLabel(role)}
    </span>
  );
}

export function TopologyPanel({ topology }: { topology: Topology }) {
  const placed = layout(topology.nodes);
  const links = edges(placed, topology.leader);
  const memberCount = topology.nodes.length;
  const onlineCount = topology.nodes.filter((n) => n.online && !n.excluded)
    .length;
  const dualRole = topology.nodes.filter(
    (n) => n.roles.includes("controller") && n.roles.includes("broker"),
  ).length;
  const controllers = topology.nodes.filter((n) =>
    n.roles.includes("controller"),
  ).length;
  const brokers = topology.nodes.filter((n) => n.roles.includes("broker"))
    .length;
  const splitPlanes = controllers > 0 && brokers > 0;
  const title =
    memberCount <= 1 && topology.leader == null
      ? "节点拓扑"
      : `集群拓扑 · ${memberCount} 成员`;

  return (
    <Panel
      title={title}
      extra={
        <div className="flex flex-wrap items-center gap-2">
          <Badge active={onlineCount === memberCount && memberCount > 0}>
            {onlineCount}/{memberCount} 健康
          </Badge>
          {topology.leader != null && <Badge>Leader #{topology.leader}</Badge>}
          <Badge>{topology.data_storage || "storage"}</Badge>
        </div>
      }
    >
      <div className="grid gap-0 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.9fr)]">
        <div className="relative min-h-[440px] overflow-hidden border-b xl:border-r xl:border-b-0">
          <div className="absolute top-3 left-4 z-10 flex flex-wrap gap-2 text-[10px] text-muted-foreground">
            <span className="rounded border bg-white/90 px-2 py-1">
              Controller {controllers}
            </span>
            <span className="rounded border bg-white/90 px-2 py-1">
              Broker {brokers}
            </span>
            {dualRole > 0 && (
              <span className="rounded border bg-white/90 px-2 py-1">
                双角色 {dualRole}
              </span>
            )}
          </div>
          <svg
            viewBox="0 0 680 440"
            className="h-[440px] w-full"
            role="img"
            aria-label="节点拓扑图"
          >
            <defs>
              <radialGradient id="topo-glow" cx="50%" cy="48%" r="55%">
                <stop offset="0%" stopColor="#059669" stopOpacity="0.10" />
                <stop offset="100%" stopColor="#059669" stopOpacity="0" />
              </radialGradient>
              <filter id="topo-shadow" x="-20%" y="-20%" width="140%" height="140%">
                <feDropShadow
                  dx="0"
                  dy="1"
                  stdDeviation="2"
                  floodColor="#0f172a"
                  floodOpacity="0.08"
                />
              </filter>
            </defs>
            <rect width="680" height="440" fill="url(#topo-glow)" />
            {splitPlanes && (
              <>
                <text
                  x="24"
                  y="48"
                  className="fill-violet-500 text-[10px] font-semibold"
                >
                  控制面 · Raft 仲裁
                </text>
                <text
                  x="24"
                  y="248"
                  className="fill-sky-600 text-[10px] font-semibold"
                >
                  数据面 · 副本组
                </text>
                <line
                  x1="40"
                  y1="214"
                  x2="640"
                  y2="214"
                  stroke="#e2e8f0"
                  strokeDasharray="4 6"
                />
              </>
            )}
            <text
              x="340"
              y="206"
              textAnchor="middle"
              className="fill-slate-400 text-[11px]"
            >
              Raft term {topology.term}
            </text>
            <text
              x="340"
              y="224"
              textAnchor="middle"
              className="fill-slate-400 text-[10px]"
            >
              rev {topology.revision.toLocaleString()}
            </text>
            {links.map(({ from, to, kind }) => {
              const style = edgeStroke(kind);
              return (
                <line
                  key={`${kind}-${from.node.id}-${to.node.id}`}
                  x1={from.x}
                  y1={from.y}
                  x2={to.x}
                  y2={to.y}
                  stroke={style.stroke}
                  strokeWidth={style.width}
                  strokeDasharray={style.dash}
                  opacity={kind === "metadata" ? 0.85 : 0.7}
                />
              );
            })}
            {placed.map(({ node, x, y }) => {
              const hasController = node.roles.includes("controller");
              const hasBroker = node.roles.includes("broker");
              return (
                <g key={node.id} transform={`translate(${x}, ${y})`}>
                  <rect
                    x={-78}
                    y={-52}
                    width={156}
                    height={118}
                    rx={16}
                    className={
                      node.excluded
                        ? "fill-orange-50 stroke-orange-200"
                        : !node.online
                          ? "fill-slate-50 stroke-slate-200"
                          : node.leader
                            ? "fill-emerald-50 stroke-emerald-300"
                            : "fill-white stroke-slate-200"
                    }
                    strokeWidth="1.5"
                    filter="url(#topo-shadow)"
                  />
                  {node.self && (
                    <rect
                      x={-84}
                      y={-58}
                      width={168}
                      height={130}
                      rx={18}
                      fill="none"
                      stroke="#059669"
                      strokeDasharray="4 4"
                      strokeWidth="1.5"
                    />
                  )}
                  <circle
                    cx={-48}
                    cy={-24}
                    r={16}
                    className={
                      node.leader
                        ? "fill-emerald-500"
                        : node.online
                          ? "fill-slate-800"
                          : "fill-slate-400"
                    }
                  />
                  <text
                    x={-48}
                    y={-23}
                    textAnchor="middle"
                    dominantBaseline="central"
                    className="fill-white text-[11px] font-semibold"
                  >
                    #{node.id}
                  </text>
                  <text
                    x={-24}
                    y={-30}
                    className="fill-slate-900 text-[12px] font-semibold"
                  >
                    成员 #{node.id}
                  </text>
                  <text x={-24} y={-12} className="fill-slate-500 text-[9px]">
                    {node.leader ? "Raft Leader" : healthLabel(node)}
                    {node.self ? " · 本机" : ""}
                  </text>
                  <g transform="translate(-66, 8)">
                    {hasController && (
                      <g>
                        <rect
                          width={58}
                          height={18}
                          rx={4}
                          className="fill-violet-100 stroke-violet-200"
                        />
                        <text
                          x={29}
                          y={13}
                          textAnchor="middle"
                          className="fill-violet-800 text-[8px] font-semibold"
                        >
                          Controller
                        </text>
                      </g>
                    )}
                    {hasBroker && (
                      <g transform={`translate(${hasController ? 64 : 0}, 0)`}>
                        <rect
                          width={48}
                          height={18}
                          rx={4}
                          className="fill-sky-100 stroke-sky-200"
                        />
                        <text
                          x={24}
                          y={13}
                          textAnchor="middle"
                          className="fill-sky-800 text-[8px] font-semibold"
                        >
                          Broker
                        </text>
                      </g>
                    )}
                  </g>
                  <text
                    x={-66}
                    y={48}
                    className="fill-slate-500 text-[9px]"
                  >
                    {hasController && !hasBroker
                      ? node.leader
                        ? "写入主 · 元数据仲裁"
                        : "跟随 · 元数据仲裁"
                      : `客户端 ${node.clients} · 租约 ${leaseLabel(node.lease_remaining_ms)}`}
                  </text>
                </g>
              );
            })}
            {placed.length === 0 && (
              <text
                x="340"
                y="210"
                textAnchor="middle"
                className="fill-slate-400 text-[12px]"
              >
                暂无节点元数据
              </text>
            )}
          </svg>
          <div className="flex flex-wrap gap-3 border-t px-5 py-3 text-[10px] text-muted-foreground">
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block h-0.5 w-4 bg-violet-500" />
              Raft 仲裁
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block h-0.5 w-4 bg-sky-400" />
              数据副本
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block h-0.5 w-4 border-t border-dashed border-emerald-400" />
              元数据 / 租约 → Leader
            </span>
            <span>虚线框 = 本机</span>
          </div>
        </div>
        <div className="flex flex-col">
          <div className="grid grid-cols-2 gap-3 border-b px-5 py-4 text-[11px] text-muted-foreground">
            <div>
              <div className="mb-1">Revision</div>
              <b className="text-sm text-foreground tabular-nums">
                {topology.revision.toLocaleString()}
              </b>
            </div>
            <div>
              <div className="mb-1">Term / Applied</div>
              <b className="text-sm text-foreground tabular-nums">
                {topology.term.toLocaleString()} /{" "}
                {topology.last_applied.toLocaleString()}
              </b>
            </div>
            <div>
              <div className="mb-1">目录客户端</div>
              <b className="text-sm text-foreground tabular-nums">
                {topology.clients.toLocaleString()}
              </b>
            </div>
            <div>
              <div className="mb-1">路由过滤</div>
              <b className="text-sm text-foreground tabular-nums">
                {topology.routes.toLocaleString()}
              </b>
            </div>
          </div>
          <ul className="max-h-[360px] space-y-3 overflow-auto px-5 py-4">
            {topology.nodes.map((node) => (
              <li
                key={node.id}
                className={`rounded-xl border px-3.5 py-3 ${nodeTone(node)}`}
              >
                <div className="mb-2 flex flex-wrap items-center gap-2 text-xs font-semibold">
                  <span>成员 #{node.id}</span>
                  {node.self && (
                    <span className="rounded bg-white/80 px-1.5 py-0.5 text-[9px]">
                      本机
                    </span>
                  )}
                  {node.leader && (
                    <span className="rounded bg-white/80 px-1.5 py-0.5 text-[9px]">
                      Leader
                    </span>
                  )}
                  <span className="ml-auto text-[10px] font-medium">
                    {healthLabel(node)}
                  </span>
                </div>
                <div className="mb-2 flex flex-wrap gap-1.5">
                  {node.roles.map((role) => (
                    <RoleChip key={role} role={role} />
                  ))}
                  <span className="rounded border border-transparent px-1.5 py-0.5 text-[9px] text-muted-foreground">
                    {roleSummary(node)}
                  </span>
                </div>
                <div className="space-y-1 font-mono text-[10px] opacity-85">
                  {node.control_address && (
                    <div>control {shortAddress(node.control_address)}</div>
                  )}
                  {node.broker_address && (
                    <div>broker {shortAddress(node.broker_address)}</div>
                  )}
                  {!node.control_address && !node.broker_address && (
                    <div>{shortAddress(node.address)}</div>
                  )}
                </div>
                <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-[10px] opacity-80">
                  {node.controller_id != null && (
                    <span>ctrl #{node.controller_id}</span>
                  )}
                  {node.broker_id != null && (
                    <span>broker #{node.broker_id}</span>
                  )}
                  {node.roles.includes("broker") && (
                    <>
                      <span>clients {node.clients.toLocaleString()}</span>
                      <span>applied {node.applied.toLocaleString()}</span>
                      {topology.revision > 0 && (
                        <span>
                          落后{" "}
                          {Math.max(
                            0,
                            topology.revision - node.applied,
                          ).toLocaleString()}
                        </span>
                      )}
                      <span>租约 {leaseLabel(node.lease_remaining_ms)}</span>
                      {node.disk_id && (
                        <span>disk {node.disk_id.slice(0, 8)}</span>
                      )}
                    </>
                  )}
                  {node.roles.includes("controller") &&
                    !node.roles.includes("broker") && (
                      <span>
                        {node.leader
                          ? "Raft 写入主"
                          : "Raft 跟随者"}
                      </span>
                    )}
                </div>
              </li>
            ))}
          </ul>
          {(topology.repair || topology.last_repair) && (
            <div className="mt-auto border-t px-5 py-3 text-[10px] text-muted-foreground">
              {topology.repair ? (
                <span>
                  修复进行中 · target #{topology.repair.target} ·{" "}
                  {topology.repair.state}
                </span>
              ) : (
                <span>
                  最近修复 · target #{topology.last_repair?.target} ·{" "}
                  {topology.last_repair?.state}
                </span>
              )}
            </div>
          )}
        </div>
      </div>
    </Panel>
  );
}
