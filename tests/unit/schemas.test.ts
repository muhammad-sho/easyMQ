import { describe, expect, it } from "vitest";
import { createJobSchema } from "../../src/api/schemas/jobs.js";
import { upsertScheduleSchema } from "../../src/api/schemas/schedules.js";

const EXECUTION = { type: "http", url: "https://example.com/hook" } as const;

describe("createJobSchema", () => {
  it("accepts a minimal immediate job", () => {
    const parsed = createJobSchema.parse({ queue: "emails", execution: EXECUTION });
    expect(parsed.queue).toBe("emails");
    expect(parsed.execution.type).toBe("http");
  });

  it("rejects the removed per-job network-policy override", () => {
    expect(() =>
      createJobSchema.parse({
        queue: "emails",
        execution: { ...EXECUTION, allowPrivateNetwork: true },
      }),
    ).toThrow();
  });

  it("accepts a fully-specified advanced job", () => {
    const parsed = createJobSchema.parse({
      queue: "emails",
      name: "send",
      payload: { to: "a@example.com" },
      execution: {
        type: "http",
        url: "https://example.com/hook",
        method: "POST",
        headers: { "x-api-key": "secret" },
        body: { hello: "world" },
        timeoutMs: 5000,
      },
      delayMs: 1000,
      attempts: 5,
      backoff: { type: "exponential", delayMs: 1000 },
      priority: 10,
      lifo: true,
      deduplication: { id: "dedup-1", ttlMs: 60_000, replace: true },
      removeOnComplete: { count: 100 },
      removeOnFail: { ageSeconds: 3600 },
    });
    expect(parsed.attempts).toBe(5);
    expect(parsed.backoff).toEqual({ type: "exponential", delayMs: 1000 });
  });

  it("accepts debounce as an alternative to deduplication", () => {
    const parsed = createJobSchema.parse({
      queue: "q",
      execution: EXECUTION,
      debounce: { id: "d1", delayMs: 5000 },
    });
    expect(parsed.debounce?.id).toBe("d1");
  });

  it("rejects invalid input with useful errors", () => {
    expect(() => createJobSchema.parse({ queue: "", execution: EXECUTION })).toThrow();
    expect(() =>
      createJobSchema.parse({
        queue: "q",
        execution: { type: "http", url: "ftp://example.com/x" },
      }),
    ).toThrow();
    expect(() =>
      createJobSchema.parse({
        queue: "q",
        execution: EXECUTION,
        deduplication: { id: "a" },
        debounce: { id: "b", delayMs: 1000 },
      }),
    ).toThrow(/deduplication or debounce/);
    expect(() =>
      createJobSchema.parse({
        queue: "q",
        execution: EXECUTION,
        priority: 9_999_999,
      }),
    ).toThrow();
    expect(() =>
      createJobSchema.parse({
        queue: "q",
        execution: EXECUTION,
        removeOnComplete: {},
      }),
    ).toThrow(/count or ageSeconds/);
    expect(() =>
      createJobSchema.parse({
        queue: "q",
        execution: { type: "http", url: "https://x.com", method: "BREW" },
      }),
    ).toThrow();
  });
});

describe("upsertScheduleSchema", () => {
  it("accepts a cron schedule and an interval schedule", () => {
    const cron = upsertScheduleSchema.parse({
      id: "nightly",
      queue: "reports",
      pattern: "0 2 * * *",
      timezone: "Europe/Berlin",
      execution: EXECUTION,
    });
    expect(cron.pattern).toBe("0 2 * * *");
    const every = upsertScheduleSchema.parse({
      id: "poll",
      queue: "reports",
      everyMs: 60_000,
      execution: EXECUTION,
    });
    expect(every.everyMs).toBe(60_000);
  });

  it("requires exactly one of pattern or everyMs", () => {
    expect(() => upsertScheduleSchema.parse({ id: "s", queue: "q", execution: EXECUTION })).toThrow(
      /pattern.*everyMs/,
    );
    expect(() =>
      upsertScheduleSchema.parse({
        id: "s",
        queue: "q",
        pattern: "* * * * *",
        everyMs: 1000,
        execution: EXECUTION,
      }),
    ).toThrow(/pattern.*everyMs/);
  });
});
