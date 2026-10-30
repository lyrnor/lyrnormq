import {
  act,
  cleanup,
  fireEvent,
  renderHook,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, it, expect, vi } from "vitest";
import { useBroker } from "../src/useBroker";
import { ApiError, api } from "../src/api";
import { initialQuery } from "../src/model";
import type { Overview } from "../src/types";
vi.mock("../src/api", async (original) => ({
  ...(await original<typeof import("../src/api")>()),
  api: vi.fn(),
}));
const request = vi.mocked(api);
const overview = (n: number) =>
  ({
    ready: true,
    uptime_seconds: n,
    messages_in_total: n * 10,
    messages_out_total: n * 20,
    sampled_at_millis: n * 1000,
    instance: {},
  }) as Overview;
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  request.mockReset();
});
describe("bounded visible-page polling", () => {
  it("loads live overview and page, calculates a rate only after a subsequent sample", async () => {
    let n = 10;
    request.mockImplementation(
      async (path) =>
        (path.includes("overview")
          ? overview(n)
          : path.includes("configuration")
            ? { instance: {}, limits: {} }
            : { items: [], total: 0, offset: 0, limit: 25 }) as never,
    );
    const expired = vi.fn();
    const { result, rerender } = renderHook(
      ({ refresh }) =>
        useBroker(
          "token",
          "connections",
          initialQuery,
          "subscriptions",
          refresh,
          expired,
        ),
      { initialProps: { refresh: 0 } },
    );
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.sessions?.total).toBe(0);
    expect(result.current.history).toEqual([]);
    n = 15;
    rerender({ refresh: 1 });
    await waitFor(() => expect(result.current.history).toHaveLength(1));
    expect(result.current.history[0].incoming).toBe(10);
  });
  it("aborts in-flight requests on route change and unmount", async () => {
    const signals: AbortSignal[] = [];
    request.mockImplementation((_path, _token, signal) => {
      signals.push(signal!);
      return new Promise<never>(() => {});
    });
    const expired = vi.fn();
    const { rerender, unmount } = renderHook(
      ({ route }) =>
        useBroker("token", route, initialQuery, "subscriptions", 0, expired),
      { initialProps: { route: "overview" as "overview" | "topics" } },
    );
    expect(signals).toHaveLength(2);
    const old = [...signals];
    rerender({ route: "topics" });
    expect(old.every((s) => s.aborted)).toBe(true);
    unmount();
    expect(signals.every((s) => s.aborted)).toBe(true);
  });
  it("never overlaps timer polls while a request is unresolved", async () => {
    vi.useFakeTimers();
    request.mockImplementation(() => new Promise<never>(() => {}));
    const { unmount } = renderHook(() =>
      useBroker("token", "overview", initialQuery, "subscriptions", 0, vi.fn()),
    );
    expect(request).toHaveBeenCalledTimes(2);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20000);
    });
    expect(request).toHaveBeenCalledTimes(2);
    unmount();
  });
  it("returns to login on a 401 rather than retaining an authenticated session", async () => {
    request.mockRejectedValue(new ApiError(401, "expired"));
    const expired = vi.fn();
    renderHook(() =>
      useBroker("token", "overview", initialQuery, "subscriptions", 0, expired),
    );
    await waitFor(() => expect(expired).toHaveBeenCalledOnce());
  });
  it("marks old snapshots stale when subsequent requests fail", async () => {
    request.mockImplementation(
      async (path) =>
        (path.includes("overview")
          ? overview(10)
          : { instance: {}, limits: {} }) as never,
    );
    const expired = vi.fn();
    const { result, rerender } = renderHook(
      ({ refresh }) =>
        useBroker(
          "token",
          "overview",
          initialQuery,
          "subscriptions",
          refresh,
          expired,
        ),
      { initialProps: { refresh: 0 } },
    );
    await waitFor(() => expect(result.current.updated).not.toBeNull());
    request.mockRejectedValue(new Error("network"));
    rerender({ refresh: 1 });
    await waitFor(() => expect(result.current.error).toBeTruthy());
    expect(result.current.overview?.messages_in_total).toBe(100);
    expect(result.current.updated).not.toBeNull();
  });
  it("pauses polling when hidden and refreshes on becoming visible", async () => {
    const hidden = vi.spyOn(document, "hidden", "get").mockReturnValue(true);
    request.mockImplementation(
      async (path) =>
        (path.includes("overview")
          ? overview(10)
          : { instance: {}, limits: {} }) as never,
    );
    const expired = vi.fn();
    const { result } = renderHook(() =>
      useBroker("token", "overview", initialQuery, "subscriptions", 0, expired),
    );
    expect(request).not.toHaveBeenCalled();
    hidden.mockReturnValue(false);
    fireEvent(document, new Event("visibilitychange"));
    await waitFor(() => expect(result.current.overview).not.toBeNull());
    expect(request).toHaveBeenCalledTimes(2);
  });
});
