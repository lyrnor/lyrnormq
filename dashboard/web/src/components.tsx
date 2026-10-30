import { useEffect, useRef, type ReactNode } from "react";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpRight,
  Activity,
  Layers3,
  ShieldCheck,
  Plug,
  SlidersHorizontal,
  TerminalSquare,
  Workflow,
  RefreshCw,
  X,
  Menu,
  Search,
  Users,
  LayoutDashboard,
  type LucideIcon,
} from "lucide-react";
import type { Page, Rate } from "./types";
import { pageBounds } from "./model";
import { Button } from "./components/ui/button";
import { Badge as UIBadge } from "./components/ui/badge";
import { Card, CardHeader, CardTitle } from "./components/ui/card";
export function Icon({ name }: { name: string }) {
  const icons: Record<string, LucideIcon> = {
    overview: LayoutDashboard,
    connections: Users,
    topics: Layers3,
    security: ShieldCheck,
    listeners: Plug,
    configuration: SlidersHorizontal,
    logs: TerminalSquare,
    rules: Workflow,
    refresh: RefreshCw,
    close: X,
    menu: Menu,
    search: Search,
    down: ArrowDown,
    up: ArrowUp,
    chevron: ArrowUpRight,
    activity: Activity,
  };
  const Component = icons[name] ?? LayoutDashboard;
  return (
    <Component
      className="size-4 shrink-0"
      strokeWidth={1.7}
      aria-hidden="true"
    />
  );
}
export function Heading({
  title,
  eyebrow,
  description,
  children,
}: {
  title: string;
  eyebrow: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <div className="mb-6 flex items-start justify-between gap-4">
      <div>
        <div className="mb-2 text-[9px] font-semibold tracking-[.18em] text-muted-foreground">
          {eyebrow}
        </div>
        <h1 className="mb-2 text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="max-w-xl text-xs leading-relaxed text-muted-foreground">
          {description}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-2">{children}</div>
    </div>
  );
}
export function Panel({
  title,
  children,
  extra,
  className = "",
}: {
  title: string;
  children: ReactNode;
  extra?: ReactNode;
  className?: string;
}) {
  return (
    <Card className={`gap-0 overflow-hidden py-0 shadow-none ${className}`}>
      <CardHeader className="flex flex-row items-center justify-between gap-3 px-5 pt-5 pb-0">
        <CardTitle className="text-sm font-semibold">
          <h2>{title}</h2>
        </CardTitle>
        {extra}
      </CardHeader>
      {children}
    </Card>
  );
}
export function Badge({
  children,
  active = true,
}: {
  children: ReactNode;
  active?: boolean;
}) {
  return (
    <UIBadge
      variant="secondary"
      className={
        active
          ? "border-transparent bg-accent text-primary text-[10px] font-medium"
          : "border-transparent bg-muted text-muted-foreground text-[10px] font-medium"
      }
    >
      {children}
    </UIBadge>
  );
}
export function KeyValues({ items }: { items: [string, ReactNode][] }) {
  return (
    <dl className="space-y-3 py-4 text-xs">
      {items.map(([key, value]) => (
        <div className="flex justify-between gap-5 leading-relaxed" key={key}>
          <dt className="min-w-24 text-muted-foreground">{key}</dt>
          <dd className="min-w-0 text-right break-words text-foreground/80">
            {value}
          </dd>
        </div>
      ))}
    </dl>
  );
}
export function Paging({
  data,
  onPage,
}: {
  data: Page<unknown>;
  onPage: (offset: number) => void;
}) {
  const b = pageBounds(data.total, data.offset, data.limit);
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-t px-5 py-3 text-xs text-muted-foreground">
      <span>
        显示 {b.start}–{b.end} 条，共 {data.total.toLocaleString()} 条
      </span>
      <div className="flex gap-2">
        <Button
          variant="outline"
          size="sm"
          disabled={!b.previous}
          onClick={() => onPage(Math.max(0, data.offset - data.limit))}
        >
          上一页
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={!b.next}
          onClick={() => onPage(data.offset + data.limit)}
        >
          下一页
        </Button>
      </div>
    </div>
  );
}
export function Empty({ children }: { children: ReactNode }) {
  return (
    <div className="px-5 py-12 text-center text-xs leading-relaxed text-muted-foreground">
      {children}
    </div>
  );
}
// Native modal semantics avoid runtime style injection under the broker's strict CSP.
export function Drawer({
  title,
  subtitle,
  onClose,
  children,
}: {
  title: string;
  subtitle: string;
  onClose: () => void;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const element = ref.current;
    const locked = document.body.classList.contains("overflow-hidden");
    document.body.classList.add("overflow-hidden");
    element?.showModal();
    element?.querySelector("button")?.focus();
    return () => {
      element?.close();
      if (!locked) document.body.classList.remove("overflow-hidden");
      previous?.focus();
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className="fixed inset-y-0 right-0 left-auto m-0 h-dvh max-h-none w-[440px] max-w-full overflow-y-auto border-0 bg-card p-0 text-foreground shadow-2xl"
      aria-labelledby="detail-title"
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
      onClick={(e) => {
        if (e.target !== e.currentTarget) return;
        const r = e.currentTarget.getBoundingClientRect();
        if (
          e.clientX < r.left ||
          e.clientX > r.right ||
          e.clientY < r.top ||
          e.clientY > r.bottom
        )
          onClose();
      }}
      onKeyDown={(e) => {
        if (e.defaultPrevented) return;
        if (e.key === "Escape") {
          e.preventDefault();
          onClose();
        }
        if (e.key === "Tab") {
          const nodes = ref.current?.querySelectorAll<HTMLElement>(
            'button,input,a,select,[tabindex="0"]',
          );
          if (!nodes?.length) return;
          const first = nodes[0],
            last = nodes[nodes.length - 1];
          if (e.shiftKey && document.activeElement === first) {
            e.preventDefault();
            last.focus();
          }
          if (!e.shiftKey && document.activeElement === last) {
            e.preventDefault();
            first.focus();
          }
        }
      }}
    >
      <div className="flex items-start justify-between gap-4 border-b p-6">
        <div className="min-w-0">
          <h2 id="detail-title" className="mb-2 text-lg font-semibold">
            {title}
          </h2>
          <p className="break-all text-xs text-muted-foreground">{subtitle}</p>
        </div>
        <Button
          variant="ghost"
          size="icon"
          className="size-8 shrink-0"
          aria-label="关闭详情"
          onClick={onClose}
        >
          <X className="size-4" />
        </Button>
      </div>
      <div className="p-6">{children}</div>
    </dialog>
  );
}
export function Traffic({ history }: { history: Rate[] }) {
  const max = Math.max(1, ...history.flatMap((p) => [p.incoming, p.outgoing]));
  const points = (key: "incoming" | "outgoing") =>
    history
      .map(
        (p, i) =>
          `${45 + (i * 555) / Math.max(1, history.length - 1)},${160 - (p[key] / max) * 140}`,
      )
      .join(" ");
  return (
    <div className="h-56 px-4 py-2">
      {history.length ? (
        <svg
          className="h-full w-full"
          viewBox="0 0 628 194"
          role="img"
          aria-label="实时消息吞吐趋势，绿色入站、蓝色出站"
        >
          {[0, 0.5, 1].map((r) => (
            <g key={r}>
              <line
                x1="45"
                x2="600"
                y1={160 - r * 140}
                y2={160 - r * 140}
                stroke="#edf1f3"
              />
              <text
                x="35"
                y={164 - r * 140}
                textAnchor="end"
                fill="#84939e"
                fontSize="9"
              >
                {Math.round(max * r).toLocaleString()}
              </text>
            </g>
          ))}
          <polyline
            points={points("outgoing")}
            fill="none"
            stroke="#91aaca"
            strokeWidth="2"
          />
          <polyline
            points={points("incoming")}
            fill="none"
            stroke="#489a79"
            strokeWidth="2"
          />
          <text x="45" y="186" fill="#84939e" fontSize="9">
            {new Date(history[0].at).toLocaleTimeString()}
          </text>
          <text x="600" y="186" textAnchor="end" fill="#84939e" fontSize="9">
            {new Date(history[history.length - 1].at).toLocaleTimeString()}
          </text>
        </svg>
      ) : (
        <Empty>等待两个成功采样点以计算消息速率</Empty>
      )}
    </div>
  );
}
