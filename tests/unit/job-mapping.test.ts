import { describe, expect, it } from "vitest";
import { ApiError } from "../../src/api/errors.js";
import { cancelledError, parseStoredData, toBullMQJobOptions } from "../../src/jobs/job-service.js";

const DEFAULTS = {
  attempts: 3,
  backoffType: "exponential" as const,
  backoffDelayMs: 5000,
  removeOnCompleteCount: 1000,
  removeOnFailCount: 5000,
};

const EXECUTION = { type: "http" as const, url: "https://example.com/hook" };

describe("toBullMQJobOptions", () => {
  it("maps defaults for a minimal job", () => {
    const opts = toBullMQJobOptions({ queue: "q", payload: null, execution: EXECUTION }, DEFAULTS);
    expect(opts.attempts).toBe(3);
    expect(opts.backoff).toEqual({ type: "exponential", delay: 5000 });
    expect(opts.removeOnComplete).toEqual({ count: 1000 });
    expect(opts.removeOnFail).toEqual({ count: 5000 });
    expect(opts.delay).toBeUndefined();
    expect(opts.priority).toBeUndefined();
  });

  it("maps explicit attempts, backoff, priority, delay and lifo", () => {
    const opts = toBullMQJobOptions(
      {
        queue: "q",
        payload: null,
        execution: EXECUTION,
        attempts: 5,
        backoff: { type: "fixed", delayMs: 1000 },
        priority: 100,
        delayMs: 2000,
        lifo: true,
        jobId: "custom-id",
      },
      DEFAULTS,
    );
    expect(opts).toMatchObject({
      attempts: 5,
      backoff: { type: "fixed", delay: 1000 },
      priority: 100,
      delay: 2000,
      lifo: true,
      jobId: "custom-id",
    });
  });

  it("maps deduplication and debounce (via deduplication)", () => {
    const dedup = toBullMQJobOptions(
      {
        queue: "q",
        payload: null,
        execution: EXECUTION,
        deduplication: { id: "d1", ttlMs: 30_000, replace: true },
      },
      DEFAULTS,
    );
    expect(dedup.deduplication).toEqual({ id: "d1", ttl: 30_000, replace: true });

    const debounce = toBullMQJobOptions(
      {
        queue: "q",
        payload: null,
        execution: EXECUTION,
        debounce: { id: "d2", delayMs: 5000 },
      },
      DEFAULTS,
    );
    expect(debounce.delay).toBe(5000);
    expect(debounce.deduplication).toEqual({ id: "d2", ttl: 5000, replace: true });
  });

  it("maps retention overrides", () => {
    const opts = toBullMQJobOptions(
      {
        queue: "q",
        payload: null,
        execution: EXECUTION,
        removeOnComplete: { count: 10, ageSeconds: 60 },
        removeOnFail: { ageSeconds: 3600 },
      },
      DEFAULTS,
    );
    expect(opts.removeOnComplete).toEqual({ count: 10, age: 60 });
    expect(opts.removeOnFail).toEqual({ age: 3600 });
  });

  it("rejects invalid priority and conflicting dedup/debounce", () => {
    expect(() =>
      toBullMQJobOptions(
        { queue: "q", payload: null, execution: EXECUTION, priority: -1 },
        DEFAULTS,
      ),
    ).toThrow(ApiError);
    expect(() =>
      toBullMQJobOptions(
        {
          queue: "q",
          payload: null,
          execution: EXECUTION,
          deduplication: { id: "a" },
          debounce: { id: "b", delayMs: 100 },
        },
        DEFAULTS,
      ),
    ).toThrow(ApiError);
  });
});

describe("parseStoredData", () => {
  it("extracts payload and execution from versioned data", () => {
    const { payload, execution } = parseStoredData({
      version: 1,
      payload: { a: 1 },
      execution: EXECUTION,
    });
    expect(payload).toEqual({ a: 1 });
    expect(execution).toEqual(EXECUTION);
  });

  it("returns nulls for foreign or malformed data", () => {
    expect(parseStoredData(null)).toEqual({ payload: null, execution: null });
    expect(parseStoredData({ version: 2 })).toEqual({ payload: null, execution: null });
    expect(parseStoredData({ version: 1, execution: { type: "smtp" } })).toEqual({
      payload: null,
      execution: null,
    });
  });
});

describe("cancelledError", () => {
  it("is unrecoverable with the stable cancellation reason", () => {
    const err = cancelledError();
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("UnrecoverableError");
    expect(err.message).toBe("easymq:cancelled");
  });
});
