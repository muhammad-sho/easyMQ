import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  buildTestSystem,
  closeTestSystem,
  uniquePrefix,
  waitForJob,
  type TestSystem,
} from "./helpers.js";

describe("distributed cancellation", () => {
  const prefix = uniquePrefix("cancel");
  let system: TestSystem;

  beforeAll(async () => {
    system = buildTestSystem(prefix);
    system.fake.setBehavior({ kind: "block" });
    await system.connections.waitUntilReady();
    await system.manager.start();
  });

  afterAll(async () => {
    await closeTestSystem(system);
  });

  it("cancels an active job without retrying", async () => {
    const queue = `q-${prefix}-cancel`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      attempts: 3,
    });
    // Wait until the worker has picked the job up (it blocks inside).
    await waitForJob(system.jobService, queue, created.id, "active");

    await system.jobService.cancelJob(queue, created.id);

    const failed = await waitForJob(system.jobService, queue, created.id, "failed");
    expect(failed.failedReason).toContain("easymq:cancelled");
    // The single started attempt counts (BullMQ v6), and no retry happened.
    expect(failed.attemptsMade).toBe(1);
    expect(system.fake.calls.filter((c) => c.jobId === created.id)).toHaveLength(1);
  });

  it("cancellation is idempotent", async () => {
    const queue = `q-${prefix}-cancel-idem`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    await waitForJob(system.jobService, queue, created.id, "active");
    await system.jobService.cancelJob(queue, created.id);
    await waitForJob(system.jobService, queue, created.id, "failed");
    // Second request after terminal state reports the race honestly.
    await expect(system.jobService.cancelJob(queue, created.id)).rejects.toMatchObject({
      code: "JOB_NOT_ACTIVE",
    });
  });

  it("refuses to cancel non-active jobs", async () => {
    const queue = `q-${prefix}-cancel-waiting`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      delayMs: 60_000,
    });
    await expect(system.jobService.cancelJob(queue, created.id)).rejects.toMatchObject({
      code: "JOB_NOT_ACTIVE",
    });
    await expect(system.jobService.cancelJob(queue, "missing-id")).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
  });
});
