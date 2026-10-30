import { useEffect, useRef, useState } from "react";
import { api, ApiError, errorMessage } from "./api";
import { addRate, queryString } from "./model";
import type {
  Configuration,
  Overview,
  Page,
  Query,
  Rate,
  Retained,
  Route,
  Session,
  Subscription,
} from "./types";
export function useBroker(
  token: string,
  route: Route,
  query: Query,
  topicTab: string,
  refresh: number,
  expired: () => void,
) {
  const [overview, setOverview] = useState<Overview | null>(null),
    [configuration, setConfiguration] = useState<Configuration | null>(null);
  const [sessions, setSessions] = useState<Page<Session> | null>(null),
    [subscriptions, setSubscriptions] = useState<Page<Subscription> | null>(
      null,
    ),
    [retained, setRetained] = useState<Page<Retained> | null>(null);
  const [history, setHistory] = useState<Rate[]>([]),
    [error, setError] = useState<string | null>(null),
    [loading, setLoading] = useState(true),
    [updated, setUpdated] = useState<number | null>(null);
  const previous = useRef<Overview | null>(null);
  const qs = queryString(query);
  useEffect(() => {
    const controller = new AbortController();
    let running = false;
    setSessions(null);
    setSubscriptions(null);
    setRetained(null);
    setLoading(true);
    setError(null);
    async function poll() {
      if (running || document.hidden || controller.signal.aborted) return;
      running = true;
      try {
        const [next, config] = await Promise.all([
          api<Overview>("/api/v1/overview", token, controller.signal),
          api<Configuration>("/api/v1/configuration", token, controller.signal),
        ]);
        if (controller.signal.aborted) return;
        let data: Page<Session> | Page<Subscription> | Page<Retained> | null =
          null;
        if (route === "connections")
          data = await api<Page<Session>>(
            `/api/v1/sessions?${qs}`,
            token,
            controller.signal,
          );
        if (route === "topics")
          data =
            topicTab === "retained"
              ? await api<Page<Retained>>(
                  `/api/v1/retained?${qs}`,
                  token,
                  controller.signal,
                )
              : await api<Page<Subscription>>(
                  `/api/v1/subscriptions?${qs}`,
                  token,
                  controller.signal,
                );
        if (controller.signal.aborted) return;
        const before = previous.current;
        setHistory((old) => addRate(before, next, old));
        previous.current = next;
        setOverview(next);
        setConfiguration(config);
        if (route === "connections") setSessions(data as Page<Session>);
        if (route === "topics" && topicTab === "retained")
          setRetained(data as Page<Retained>);
        if (route === "topics" && topicTab !== "retained")
          setSubscriptions(data as Page<Subscription>);
        setUpdated(Date.now());
        setError(null);
      } catch (cause) {
        if (controller.signal.aborted) return;
        if (cause instanceof ApiError && cause.status === 401) {
          expired();
          return;
        }
        setError(errorMessage(cause));
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
          running = false;
        }
      }
    }
    void poll();
    const timer = window.setInterval(() => void poll(), 5000);
    const visible = () => {
      if (!document.hidden) void poll();
    };
    document.addEventListener("visibilitychange", visible);
    return () => {
      controller.abort();
      clearInterval(timer);
      document.removeEventListener("visibilitychange", visible);
    };
  }, [token, route, qs, topicTab, refresh, expired]);
  return {
    overview,
    configuration,
    sessions,
    subscriptions,
    retained,
    history,
    error,
    loading,
    updated,
  };
}
