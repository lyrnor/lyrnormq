import { useEffect, useRef, type ReactNode } from "react";
import type { Page, Rate } from "./types";
import { pageBounds } from "./model";
export function Icon({ name }: { name: string }) {
  const paths: Record<string, ReactNode> = {
    overview: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
      </>
    ),
    connections: (
      <>
        <circle cx="9" cy="8" r="3" />
        <path d="M3 21v-3a6 6 0 0 1 12 0v3M16 5a3 3 0 0 1 0 6M18 15a5 5 0 0 1 3 5" />
      </>
    ),
    topics: (
      <>
        <path d="m12 3 10 5-10 5L2 8l10-5ZM2 12l10 5 10-5M2 16l10 5 10-5" />
      </>
    ),
    security: (
      <>
        <path d="m12 2 8 4v7c0 5-8 9-8 9s-8-4-8-9V6l8-4Z" />
        <path d="m8 12 3 3 5-6" />
      </>
    ),
    listeners: (
      <>
        <path d="M8 2v6m8-6v6M6 8h12v5a6 6 0 0 1-12 0V8Zm6 11v3" />
      </>
    ),
    configuration: (
      <>
        <path d="M3 6h18M3 12h18M3 18h18" />
        <circle cx="8" cy="6" r="2" />
        <circle cx="16" cy="12" r="2" />
        <circle cx="9" cy="18" r="2" />
      </>
    ),
    logs: (
      <>
        <rect x="3" y="4" width="18" height="16" rx="2" />
        <path d="m7 9 3 3-3 3m6 0h4" />
      </>
    ),
    rules: (
      <>
        <rect x="4" y="3" width="16" height="18" rx="2" />
        <path d="M8 8h8M8 12h8M8 16h5" />
      </>
    ),
    refresh: (
      <>
        <path d="M20 11a8 8 0 1 0-2 7M20 3v8h-8" />
      </>
    ),
    close: <path d="m5 5 14 14M19 5 5 19" />,
    menu: <path d="M4 6h16M4 12h16M4 18h16" />,
    search: (
      <>
        <circle cx="10" cy="10" r="6" />
        <path d="m15 15 6 6" />
      </>
    ),
    down: <path d="M12 3v17m-6-6 6 6 6-6" />,
    up: <path d="M12 21V4m-6 6 6-6 6 6" />,
  };
  return (
    <svg className="icon" viewBox="0 0 24 24" aria-hidden="true">
      {paths[name] ?? paths.overview}
    </svg>
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
    <div className="page-heading">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      <div className="heading-actions">{children}</div>
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
    <section className={`panel ${className}`}>
      <div className="panel-header">
        <h2 className="panel-title">{title}</h2>
        {extra}
      </div>
      {children}
    </section>
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
    <span className={`badge ${active ? "green" : "gray"}`}>{children}</span>
  );
}
export function KeyValues({ items }: { items: [string, ReactNode][] }) {
  return (
    <dl className="kv-list">
      {items.map(([key, value]) => (
        <div className="kv-pair" key={key}>
          <dt>{key}</dt>
          <dd>{value}</dd>
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
    <div className="table-foot">
      <span>
        显示 {b.start}–{b.end} 条，共 {data.total.toLocaleString()} 条
      </span>
      <div className="page-actions">
        <button
          className="btn"
          disabled={!b.previous}
          onClick={() => onPage(Math.max(0, data.offset - data.limit))}
        >
          上一页
        </button>
        <button
          className="btn"
          disabled={!b.next}
          onClick={() => onPage(data.offset + data.limit)}
        >
          下一页
        </button>
      </div>
    </div>
  );
}
export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty-table">{children}</div>;
}
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
  const ref = useRef<HTMLElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const bodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    ref.current?.querySelector("button")?.focus();
    const key = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
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
    };
    document.addEventListener("keydown", key);
    return () => {
      document.removeEventListener("keydown", key);
      document.body.style.overflow = bodyOverflow;
      previous?.focus();
    };
  }, [onClose]);
  return (
    <>
      <div className="overlay" onClick={onClose} />
      <section
        ref={ref}
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby="detail-title"
      >
        <div className="drawer-head">
          <div>
            <h2 id="detail-title">{title}</h2>
            <p>{subtitle}</p>
          </div>
          <button
            className="icon-button"
            aria-label="关闭详情"
            onClick={onClose}
          >
            <Icon name="close" />
          </button>
        </div>
        <div className="drawer-body">{children}</div>
      </section>
    </>
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
    <div className="traffic-chart">
      {history.length ? (
        <svg
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
