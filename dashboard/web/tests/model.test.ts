import { describe, it, expect } from "vitest";
import {
  addRate,
  authReducer,
  boundedSearch,
  changeQuery,
  initialQuery,
  pageBounds,
  queryString,
  protocolLabel,
  routeFromHash,
} from "../src/model";
import type { Overview } from "../src/types";
const sample = (patch: Partial<Overview> = {}): Overview =>
  ({
    sampled_at_millis: 10000,
    uptime_seconds: 100,
    messages_in_total: 100,
    messages_out_total: 200,
    ...patch,
  }) as Overview;
describe("real counter rate calculation", () => {
  it("does not invent a rate before two samples", () =>
    expect(addRate(null, sample(), [])).toEqual([]));
  it("uses elapsed milliseconds, not assumed polling frequency", () =>
    expect(
      addRate(
        sample(),
        sample({
          sampled_at_millis: 12500,
          messages_in_total: 110,
          messages_out_total: 225,
        }),
        [],
      ),
    ).toEqual([{ at: 12500, incoming: 4, outgoing: 10 }]));
  it("clears history after restart, negative counters, invalid times or unsafe integers", () => {
    const history = [{ at: 5000, incoming: 2, outgoing: 3 }];
    for (const patch of [
      { sampled_at_millis: 10000 },
      { sampled_at_millis: 9000 },
      { sampled_at_millis: 15000, uptime_seconds: 1 },
      { sampled_at_millis: 15000, messages_in_total: 99 },
      { sampled_at_millis: 15000, messages_out_total: 199 },
      {
        sampled_at_millis: 15000,
        messages_in_total: Number.MAX_SAFE_INTEGER + 1,
      },
    ])
      expect(addRate(sample(), sample(patch), history)).toEqual([]);
  });
  it("bounds retained samples to sixty", () => {
    const history = Array.from({ length: 60 }, (_, at) => ({
      at,
      incoming: 1,
      outgoing: 2,
    }));
    const next = addRate(
      sample(),
      sample({ sampled_at_millis: 15000, messages_in_total: 200 }),
      history,
    );
    expect(next).toHaveLength(60);
    expect(next[0].at).toBe(1);
  });
});
describe("query and pagination", () => {
  it("resets offset when filters change but preserves explicit page navigation", () => {
    expect(
      changeQuery({ ...initialQuery, offset: 50 }, { search: "sensor" }).offset,
    ).toBe(0);
    expect(changeQuery(initialQuery, { offset: 25 }).offset).toBe(25);
  });
  it("encodes hostile client identifiers without treating them as URL syntax", () => {
    const value = "a/#?x=<script>&";
    const query = queryString({ ...initialQuery, search: value });
    expect(new URLSearchParams(query).get("search")).toBe(value);
    expect(query).not.toContain("<script>");
  });
  it("handles empty, final and out-of-range pages", () => {
    expect(pageBounds(0, 0, 25)).toEqual({
      start: 0,
      end: 0,
      previous: false,
      next: false,
    });
    expect(pageBounds(26, 25, 25)).toEqual({
      start: 26,
      end: 26,
      previous: true,
      next: false,
    });
    expect(pageBounds(10, 25, 25).start).toBe(0);
  });
  it("restricts hash routes to known application pages", () => {
    expect(routeFromHash("#topics")).toBe("topics");
    expect(routeFromHash("#<script>")).toBe("overview");
  });
});
describe("authentication state", () => {
  it("retains only the current memory token and clears it on logout or expiry", () => {
    const logged = authReducer(
      { token: null, error: null },
      { type: "login", token: "test-token" },
    );
    expect(logged.token).toBe("test-token");
    expect(authReducer(logged, { type: "logout" })).toEqual({
      token: null,
      error: null,
    });
    expect(authReducer(logged, { type: "expired" }).token).toBeNull();
    expect(authReducer(logged, { type: "expired" }).error).toBeTruthy();
  });
});

describe("restored metadata and input bounds", () => {
  it("does not invent a version for an offline recovered session", () =>
    expect(protocolLabel("unknown")).toBe("未协商"));
  it("bounds UTF8 search terms while preserving complete code points", () => {
    expect(boundedSearch("x".repeat(300))).toHaveLength(256);
    const text = boundedSearch("温度🌡".repeat(100));
    expect(new TextEncoder().encode(text).length).toBeLessThanOrEqual(256);
    expect(text).not.toContain("�");
  });
});

it("does not navigate past the management API offset ceiling", () => {
  expect(pageBounds(2_000_000, 1_000_000, 25).next).toBe(false);
});
