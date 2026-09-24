import { describe, expect, it, vi } from "vitest";
import { ApiError } from "../../src/api/errors.js";
import type { AppConfig } from "../../src/config/env.js";
import type { QueueFactory } from "../../src/infrastructure/bullmq/queue-factory.js";
import { JobService, toPublicExecution } from "../../src/jobs/job-service.js";
import type { Execution } from "../../src/jobs/job-types.js";
import { ScheduleService } from "../../src/jobs/schedule-service.js";
import { QueueCatalog } from "../../src/queues/queue-catalog.js";
import type { Redis } from "ioredis";
import { testConfig } from "../integration/helpers.js";

const PREFIX = "unit-services";

function config(): AppConfig {
  return testConfig(PREFIX, { REDIS_KEY_PREFIX: PREFIX });
}

function fakeBullJob() {
  return {
    id: "job-1",
    name: "default",
    queueName: "q",
    data: { version: 1, payload: null, execution: { type: "http", url: "https://x/" } },
    attemptsMade: 0,
    timestamp: 1_700_000_000_000,
    opts: {},
    processedOn: undefined,
    finishedOn: undefined,
    failedReason: undefined,
    returnvalue: undefined,
    getState: vi.fn().mockResolvedValue("waiting"),
  };
}

describe("toPublicExecution", () => {
  it("exposes metadata and header names but never header values", () => {
    const stored: Execution = {
      type: "http",
      url: "https://example.com/hook",
      method: "POST",
      headers: { authorization: "Bearer s3cr3t", "x-api-key": "k3y" },
      body: { hello: "world" },
      timeoutMs: 5000,
    };
    const snapshot = JSON.stringify(stored);
    const view = toPublicExecution(stored);
    expect(view).toEqual({
      type: "http",
      url: "https://example.com/hook",
      method: "POST",
      headerNames: ["authorization", "x-api-key"],
      body: { hello: "world" },
      timeoutMs: 5000,
    });
    const serialized = JSON.stringify(view);
    expect(serialized).not.toContain("s3cr3t");
    expect(serialized).not.toContain("k3y");
    // Stored config untouched — the worker still gets the real headers.
    expect(JSON.stringify(stored)).toBe(snapshot);
    expect(stored.headers?.["authorization"]).toBe("Bearer s3cr3t");
  });

  it("omits headerNames when no headers are configured", () => {
    expect(toPublicExecution({ type: "http", url: "https://x/" })).toEqual({
      type: "http",
      url: "https://x/",
    });
  });
});

describe("JobService.createJob ordering", () => {
  function setup(addImpl?: () => Promise<unknown>) {
    const register = vi.fn().mockResolvedValue(true);
    const add = vi.fn().mockImplementation(addImpl ?? (() => Promise.resolve(fakeBullJob())));
    const catalog = { register } as unknown as QueueCatalog;
    const queues = { getQueue: vi.fn().mockReturnValue({ add }) } as unknown as QueueFactory;
    const service = new JobService(queues, catalog, config());
    return { service, register, add };
  }

  const input = {
    queue: "emails",
    execution: { type: "http", url: "https://example.com/hook" } as const,
  };

  it("registers the queue before calling Queue.add", async () => {
    const { service, register, add } = setup();
    await service.createJob({ ...input, execution: { ...input.execution } });
    expect(register).toHaveBeenCalledTimes(1);
    expect(add).toHaveBeenCalledTimes(1);
    expect(register.mock.invocationCallOrder[0]).toBeLessThan(
      add.mock.invocationCallOrder[0] as number,
    );
  });

  it("fails without enqueueing when registration is unavailable", async () => {
    const refused = Object.assign(new Error("connect ECONNREFUSED 10.9.9.9:6380"), {
      code: "ECONNREFUSED",
    });
    const brokenRedis = {
      multi: () => {
        throw refused;
      },
      smembers: () => Promise.resolve([]),
    } as unknown as Redis;
    // Real catalog: proves Redis failures map to service-unavailable.
    const catalog = new QueueCatalog(brokenRedis, () => brokenRedis, { keyPrefix: PREFIX });
    const add = vi.fn();
    const queues = { getQueue: vi.fn().mockReturnValue({ add }) } as unknown as QueueFactory;
    const service = new JobService(queues, catalog, config());
    const err = await service
      .createJob({ ...input, execution: { ...input.execution } })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("SERVICE_UNAVAILABLE");
    expect(JSON.stringify((err as ApiError).toBody())).not.toContain("10.9.9.9");
    expect(add).not.toHaveBeenCalled();
  });

  it("keeps the registration (no rollback) when enqueueing fails", async () => {
    const { service, register } = setup(() =>
      Promise.reject(
        Object.assign(new Error("boom"), { code: "EIO", name: "BackendExplodedError" }),
      ),
    );
    const err = await service
      .createJob({ ...input, execution: { ...input.execution } })
      .catch((e: unknown) => e);
    expect(register).toHaveBeenCalledTimes(1);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("INTERNAL_ERROR");
    expect(JSON.stringify((err as ApiError).toBody())).not.toContain("boom");
  });
});

describe("ScheduleService.upsertSchedule ordering", () => {
  it("registers the queue before calling upsertJobScheduler", async () => {
    const register = vi.fn().mockResolvedValue(true);
    const upsertJobScheduler = vi.fn().mockResolvedValue({});
    const getJobScheduler = vi.fn().mockResolvedValue({
      key: "sched",
      id: "sched",
      name: "sched",
    });
    const catalog = {
      register,
      list: vi.fn().mockResolvedValue(["reports"]),
    } as unknown as QueueCatalog;
    const queues = {
      getQueue: vi.fn().mockReturnValue({ upsertJobScheduler, getJobScheduler }),
    } as unknown as QueueFactory;
    const service = new ScheduleService(queues, catalog, config());
    await service.upsertSchedule({
      id: "sched",
      queue: "reports",
      everyMs: 60_000,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    expect(register).toHaveBeenCalledTimes(1);
    expect(upsertJobScheduler).toHaveBeenCalledTimes(1);
    expect(register.mock.invocationCallOrder[0]).toBeLessThan(
      upsertJobScheduler.mock.invocationCallOrder[0] as number,
    );
  });
});

describe("backend failure classification", () => {
  function serviceWithFailingQueue(failure: Error) {
    const catalog = {
      register: vi.fn().mockResolvedValue(true),
      list: vi.fn().mockResolvedValue(["q"]),
    } as unknown as QueueCatalog;
    const queues = {
      getQueue: vi.fn().mockReturnValue({
        add: vi.fn().mockRejectedValue(failure),
        getJob: vi.fn().mockRejectedValue(failure),
      }),
    } as unknown as QueueFactory;
    return new JobService(queues, catalog, config());
  }

  it("maps connection failures during enqueue to 503", async () => {
    const service = serviceWithFailingQueue(
      Object.assign(new Error("connect ECONNREFUSED 10.1.2.3:6379"), { code: "ECONNREFUSED" }),
    );
    const err = await service
      .createJob({
        queue: "q",
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("SERVICE_UNAVAILABLE");
    expect(JSON.stringify((err as ApiError).toBody())).not.toContain("10.1.2.3");
  });

  it("maps unexpected queue failures to 500 without backend wording", async () => {
    const service = serviceWithFailingQueue(
      new Error("Missing lock for job 9. failed to run clean job script"),
    );
    const err = await service.getJob("q", "9").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("INTERNAL_ERROR");
    const body = JSON.stringify((err as ApiError).toBody());
    expect(body).not.toContain("Missing lock");
    expect(body).not.toContain("clean job script");
  });
});
