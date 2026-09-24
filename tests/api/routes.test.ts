import { beforeAll, describe, expect, it, vi } from "vitest";
import type { AppInstance, ApiServices } from "../../src/api/server.js";
import { buildApp } from "../../src/api/server.js";
import { loadConfig } from "../../src/config/env.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import { HealthService } from "../../src/health/health-service.js";
import type { JobService } from "../../src/jobs/job-service.js";
import type { QueueService } from "../../src/queues/queue-service.js";
import type { ScheduleService } from "../../src/jobs/schedule-service.js";
import type { EasyMQJob } from "../../src/jobs/job-types.js";

const TOKEN = "test-token-123";

function sampleJob(): EasyMQJob {
  return {
    id: "job-1",
    queue: "emails",
    name: "default",
    state: "waiting",
    payload: { to: "a@example.com" },
    execution: { type: "http", url: "https://example.com/hook" },
    attemptsMade: 0,
    priority: 0,
    delayMs: 0,
    timestampMs: 1_700_000_000_000,
    processedOnMs: null,
    finishedOnMs: null,
    failedReason: null,
    returnValue: null,
  };
}

async function buildTestApp(overrides: {
  authDisabled?: boolean;
  readinessFails?: boolean;
} = {}): Promise<AppInstance> {
  const config = loadConfig({
    API_TOKEN: TOKEN,
    ...(overrides.authDisabled ? { AUTH_DISABLED: "true" } : {}),
  });
  const logger = createLogger({ level: "silent", role: "api" });

  const queueService = {
    listQueues: vi.fn().mockResolvedValue(["emails", "reports"]),
    getQueue: vi.fn().mockResolvedValue({ name: "emails", isPaused: false, counts: { waiting: 1 } }),
    pauseQueue: vi.fn().mockResolvedValue({ name: "emails", isPaused: true, counts: {} }),
    resumeQueue: vi.fn().mockResolvedValue({ name: "emails", isPaused: false, counts: {} }),
    getJobCounts: vi.fn().mockResolvedValue({ waiting: 1, active: 0 }),
  } as unknown as QueueService;

  const jobService = {
    createJob: vi.fn().mockImplementation(async () => sampleJob()),
    getJob: vi.fn().mockResolvedValue(sampleJob()),
    listJobs: vi.fn().mockResolvedValue({ jobs: [sampleJob()], offset: 0, limit: 50, nextOffset: null }),
    removeJob: vi.fn().mockResolvedValue(undefined),
    retryJob: vi.fn().mockResolvedValue({ ...sampleJob(), state: "waiting" }),
    promoteJob: vi.fn().mockResolvedValue(sampleJob()),
    changeJobDelay: vi.fn().mockResolvedValue(sampleJob()),
    cancelJob: vi.fn().mockResolvedValue({ ...sampleJob(), state: "active" }),
  } as unknown as JobService;

  const scheduleService = {
    upsertSchedule: vi.fn().mockResolvedValue({
      id: "nightly",
      queue: "reports",
      name: "nightly",
      pattern: "0 2 * * *",
      everyMs: null,
      timezone: null,
      nextRunAtMs: null,
      iterationCount: null,
      limit: null,
      startDateMs: null,
      endDateMs: null,
    }),
    getSchedule: vi.fn().mockResolvedValue({
      id: "nightly",
      queue: "reports",
      name: "nightly",
      pattern: "0 2 * * *",
      everyMs: null,
      timezone: null,
      nextRunAtMs: null,
      iterationCount: null,
      limit: null,
      startDateMs: null,
      endDateMs: null,
    }),
    listSchedules: vi.fn().mockResolvedValue({ schedules: [], offset: 0, limit: 50, nextOffset: null }),
    removeSchedule: vi.fn().mockResolvedValue(undefined),
  } as unknown as ScheduleService;

  const healthService = new HealthService("api");
  if (overrides.readinessFails) {
    healthService.addCheck({
      name: "redis",
      check: async () => {
        throw new Error("connection refused");
      },
    });
  }

  const services: ApiServices = {
    config,
    logger,
    queueService,
    jobService,
    scheduleService,
    healthService,
  };
  const app = await buildApp(services);
  return app;
}

function authHeaders() {
  return { authorization: `Bearer ${TOKEN}` };
}

describe("jobs API", () => {
  let app: AppInstance;
  beforeAll(async () => {
    app = await buildTestApp();
  });

  it("creates a job (201) and validates input (400)", async () => {
    const created = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: authHeaders(),
      payload: { queue: "emails", execution: { type: "http", url: "https://example.com/hook" } },
    });
    expect(created.statusCode).toBe(201);
    expect(created.json().id).toBe("job-1");

    const invalid = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: authHeaders(),
      payload: { queue: "emails", execution: { type: "http", url: "ftp://x" } },
    });
    expect(invalid.statusCode).toBe(400);
    expect(invalid.json().error.code).toBe("VALIDATION_ERROR");
  });

  it("lists, gets, retries, promotes, delays, cancels and removes jobs", async () => {
    const h = authHeaders();
    expect((await app.inject({ method: "GET", url: "/queues/emails/jobs", headers: h })).statusCode).toBe(200);
    expect((await app.inject({ method: "GET", url: "/queues/emails/jobs/job-1", headers: h })).statusCode).toBe(200);
    expect((await app.inject({ method: "POST", url: "/queues/emails/jobs/job-1/retry", headers: h })).statusCode).toBe(200);
    expect((await app.inject({ method: "POST", url: "/queues/emails/jobs/job-1/promote", headers: h })).statusCode).toBe(200);
    const delayed = await app.inject({
      method: "PATCH",
      url: "/queues/emails/jobs/job-1/delay",
      headers: h,
      payload: { delayMs: 1000 },
    });
    expect(delayed.statusCode).toBe(200);
    const cancel = await app.inject({ method: "POST", url: "/queues/emails/jobs/job-1/cancel", headers: h });
    expect(cancel.statusCode).toBe(202);
    expect((await app.inject({ method: "DELETE", url: "/queues/emails/jobs/job-1", headers: h })).statusCode).toBe(204);
  });

  it("rejects invalid state filters", async () => {
    const res = await app.inject({
      method: "GET",
      url: "/queues/emails/jobs?state=bogus",
      headers: authHeaders(),
    });
    expect(res.statusCode).toBe(400);
  });
});

describe("queues API", () => {
  let app: AppInstance;
  beforeAll(async () => {
    app = await buildTestApp();
  });

  it("lists, inspects, pauses, resumes and counts", async () => {
    const h = authHeaders();
    const list = await app.inject({ method: "GET", url: "/queues", headers: h });
    expect(list.json()).toEqual({ queues: ["emails", "reports"] });
    expect((await app.inject({ method: "GET", url: "/queues/emails", headers: h })).statusCode).toBe(200);
    const paused = await app.inject({ method: "POST", url: "/queues/emails/pause", headers: h });
    expect(paused.json().isPaused).toBe(true);
    const resumed = await app.inject({ method: "POST", url: "/queues/emails/resume", headers: h });
    expect(resumed.json().isPaused).toBe(false);
    expect((await app.inject({ method: "GET", url: "/queues/emails/counts", headers: h })).statusCode).toBe(200);
  });
});

describe("schedules API", () => {
  let app: AppInstance;
  beforeAll(async () => {
    app = await buildTestApp();
  });

  it("upserts, gets, lists and removes schedules", async () => {
    const h = authHeaders();
    const upsert = await app.inject({
      method: "POST",
      url: "/schedules",
      headers: h,
      payload: {
        id: "nightly",
        queue: "reports",
        pattern: "0 2 * * *",
        execution: { type: "http", url: "https://example.com/hook" },
      },
    });
    expect(upsert.statusCode).toBe(200);
    expect(upsert.json().id).toBe("nightly");
    expect((await app.inject({ method: "GET", url: "/queues/reports/schedules", headers: h })).statusCode).toBe(200);
    expect((await app.inject({ method: "GET", url: "/queues/reports/schedules/nightly", headers: h })).statusCode).toBe(200);
    expect((await app.inject({ method: "DELETE", url: "/queues/reports/schedules/nightly", headers: h })).statusCode).toBe(204);
  });
});

describe("auth", () => {
  let app: AppInstance;
  let openApp: AppInstance;
  beforeAll(async () => {
    app = await buildTestApp();
    openApp = await buildTestApp({ authDisabled: true });
  });

  it("requires a valid Bearer token", async () => {
    expect((await app.inject({ method: "GET", url: "/queues" })).statusCode).toBe(401);
    expect(
      (await app.inject({ method: "GET", url: "/queues", headers: { authorization: "Bearer wrong" } })).statusCode,
    ).toBe(401);
    expect(
      (await app.inject({ method: "GET", url: "/queues", headers: authHeaders() })).statusCode,
    ).toBe(200);
    // Health probes stay public.
    expect((await app.inject({ method: "GET", url: "/health/live" })).statusCode).toBe(200);
    expect((await app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(200);
    // Tokens never leak into the 401 body.
    expect(await app.inject({ method: "GET", url: "/queues" })).not.toContain?.(TOKEN);
  });

  it("can be disabled explicitly for development", async () => {
    expect((await openApp.inject({ method: "GET", url: "/queues" })).statusCode).toBe(200);
  });

  it("returns JSON 404 for unknown routes", async () => {
    const res = await app.inject({ method: "GET", url: "/nope", headers: authHeaders() });
    expect(res.statusCode).toBe(404);
    expect(res.json().error.code).toBe("NOT_FOUND");
  });
});

describe("health API", () => {
  it("reports liveness and readiness", async () => {
    const app = await buildTestApp();
    const live = await app.inject({ method: "GET", url: "/health/live" });
    expect(live.statusCode).toBe(200);
    expect(live.json().status).toBe("ok");

    const ready = await app.inject({ method: "GET", url: "/health/ready" });
    expect(ready.statusCode).toBe(200);
  });

  it("reports 503 when a readiness check fails", async () => {
    const app = await buildTestApp({ readinessFails: true });
    const ready = await app.inject({ method: "GET", url: "/health/ready" });
    expect(ready.statusCode).toBe(503);
    expect(ready.json().error.code).toBe("SERVICE_UNAVAILABLE");
  });
});
