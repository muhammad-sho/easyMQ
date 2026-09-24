import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  buildTestSystem,
  closeTestSystem,
  uniquePrefix,
  waitForJob,
  type TestSystem,
} from "./helpers.js";

describe("job lifecycle", () => {
  const prefix = uniquePrefix("jobs");
  let system: TestSystem;

  beforeAll(async () => {
    system = buildTestSystem(prefix);
    await system.connections.waitUntilReady();
    await system.manager.start();
  });

  afterAll(async () => {
    await closeTestSystem(system);
  });

  it("executes an immediate job and stores the result", async () => {
    const queue = `q-${prefix}-immediate`;
    const created = await system.jobService.createJob({
      queue,
      payload: { hello: "world" },
      execution: { type: "http", url: "https://example.com/hook" },
    });
    expect(created.state).toBe("waiting");
    expect(created.payload).toEqual({ hello: "world" });
    const done = await waitForJob(system.jobService, queue, created.id, "completed");
    // BullMQ counts the started attempt: one successful run => attemptsMade 1.
    expect(done.attemptsMade).toBe(1);
    expect(done.returnValue).toMatchObject({ statusCode: 200 });
    expect(JSON.stringify(done.returnValue)).toContain("hello");
    expect(system.fake.calls.some((c) => c.jobId === created.id)).toBe(true);
  });

  it("runs delayed jobs after promotion", async () => {
    const queue = `q-${prefix}-delayed`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      delayMs: 60_000,
    });
    expect((await system.jobService.getJob(queue, created.id)).state).toBe("delayed");
    await system.jobService.promoteJob(queue, created.id);
    await waitForJob(system.jobService, queue, created.id, "completed");
  });

  it("supports priority, attempts and backoff options", async () => {
    const queue = `q-${prefix}-opts`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      priority: 100,
      attempts: 3,
      backoff: { type: "fixed", delayMs: 100 },
    });
    const fetched = await system.jobService.getJob(queue, created.id);
    expect(fetched.priority).toBe(100);
    await waitForJob(system.jobService, queue, created.id, "completed");
  });

  it("deduplicates jobs with the same deduplication id", async () => {
    const queue = `q-${prefix}-dedup`;
    const execution = { type: "http" as const, url: "https://example.com/hook" };
    const first = await system.jobService.createJob({
      queue,
      payload: { n: 1 },
      execution,
      delayMs: 60_000,
      deduplication: { id: `dedup-${prefix}`, ttlMs: 60_000 },
    });
    const second = await system.jobService.createJob({
      queue,
      payload: { n: 2 },
      execution,
      delayMs: 60_000,
      deduplication: { id: `dedup-${prefix}`, ttlMs: 60_000 },
    });
    expect(second.id).toBe(first.id);
  });

  it("replaces debounced jobs instead of duplicating them", async () => {
    const queue = `q-${prefix}-debounce`;
    const execution = { type: "http" as const, url: "https://example.com/hook" };
    await system.jobService.createJob({
      queue,
      payload: { n: 1 },
      execution,
      debounce: { id: `deb-${prefix}`, delayMs: 60_000 },
    });
    await system.jobService.createJob({
      queue,
      payload: { n: 2 },
      execution,
      debounce: { id: `deb-${prefix}`, delayMs: 60_000 },
    });
    const page = await system.jobService.listJobs({ queue, states: ["delayed"] });
    expect(page.jobs).toHaveLength(1);
    expect(page.jobs[0]?.payload).toEqual({ n: 2 });
  });

  it("retries with backoff and then fails permanently", async () => {
    const queue = `q-${prefix}-retry`;
    system.fake.setBehavior({ kind: "fail", message: "boom" });
    try {
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        attempts: 2,
        backoff: { type: "fixed", delayMs: 200 },
      });
      const failed = await waitForJob(system.jobService, queue, created.id, "failed", 30_000);
      expect(failed.attemptsMade).toBe(2);
      expect(failed.failedReason).toContain("boom");
    } finally {
      system.fake.setBehavior({ kind: "success" });
    }
  });

  it("manually retries a failed job", async () => {
    const queue = `q-${prefix}-manual-retry`;
    system.fake.setBehavior({ kind: "failOnce" });
    try {
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        attempts: 1,
      });
      await waitForJob(system.jobService, queue, created.id, "failed");
      await system.jobService.retryJob(queue, created.id);
      const done = await waitForJob(system.jobService, queue, created.id, "completed");
      expect(done.returnValue).toMatchObject({ statusCode: 200, body: "recovered" });
    } finally {
      system.fake.setBehavior({ kind: "success" });
    }
  });

  it("removes jobs and changes delayed-job delay", async () => {
    const queue = `q-${prefix}-remove`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      delayMs: 60_000,
    });
    await system.jobService.removeJob(queue, created.id);
    await expect(system.jobService.getJob(queue, created.id)).rejects.toMatchObject({
      code: "NOT_FOUND",
    });

    const delayed = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      delayMs: 60_000,
    });
    await system.jobService.changeJobDelay(queue, delayed.id, 200);
    await waitForJob(system.jobService, queue, delayed.id, "completed");
  });

  it("lists jobs with bounded pagination", async () => {
    const queue = `q-${prefix}-paging`;
    for (let n = 0; n < 5; n++) {
      await system.jobService.createJob({
        queue,
        payload: { n },
        execution: { type: "http", url: "https://example.com/hook" },
      });
    }
    const first = await system.jobService.listJobs({ queue, limit: 2, offset: 0 });
    expect(first.jobs).toHaveLength(2);
    expect(first.limit).toBe(2);
    expect(first.nextOffset).toBe(2);
    const second = await system.jobService.listJobs({ queue, limit: 2, offset: 2 });
    expect(second.jobs).toHaveLength(2);
    expect(second.nextOffset).toBe(4);
    const ids = new Set([...first.jobs, ...second.jobs].map((j) => j.id));
    expect(ids.size).toBe(4);
  });
});
