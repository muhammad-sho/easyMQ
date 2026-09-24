import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { WorkerManager } from "../../src/workers/worker-manager.js";
import {
  buildTestSystem,
  closeTestSystem,
  uniquePrefix,
  waitFor,
  waitForJob,
  type TestSystem,
} from "./helpers.js";

function markerKey(prefix: string, queue: string, jobId: string): string {
  return `${prefix}:cancel:${queue}:${jobId}`;
}

async function jobTimestamp(system: TestSystem, queue: string, jobId: string): Promise<number> {
  const job = await system.queues.getQueue(queue).getJob(jobId);
  if (!job) throw new Error(`job ${jobId} missing`);
  return job.timestamp;
}

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
  });

  it("cancels before execution via the pre-attempt marker check", async () => {
    const queue = `q-${prefix}-pre`;
    const before = system.fake.calls.length;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      delayMs: 60_000,
    });
    // Plant a marker for the waiting job with correct generation identity,
    // then promote it: the worker must refuse the attempt before executing.
    const shared = system.connections.getShared();
    await shared.set(
      markerKey(prefix, queue, created.id),
      JSON.stringify({
        queue,
        jobId: created.id,
        jobTimestampMs: await jobTimestamp(system, queue, created.id),
        attemptsMade: 0,
        requestedAtMs: Date.now(),
      }),
      "EX",
      60,
    );
    await system.jobService.promoteJob(queue, created.id);
    const failed = await waitForJob(system.jobService, queue, created.id, "failed");
    expect(failed.failedReason).toContain("easymq:cancelled");
    expect(system.fake.calls.length).toBe(before);
  });

  it("cancels during a retry without further retries", async () => {
    system.fake.setBehavior({ kind: "failThenBlock" });
    try {
      const queue = `q-${prefix}-retry-cancel`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        attempts: 3,
      });
      // First attempt fails on its own; the retry blocks in the executor.
      await waitForJob(system.jobService, queue, created.id, "active");
      await waitFor(
        () =>
          system.fake.calls.filter((c) => c.jobId === created.id && c.attemptsMade === 1).length >
          0,
        { label: "retry attempt to start" },
      );
      await system.jobService.cancelJob(queue, created.id);
      const failed = await waitForJob(system.jobService, queue, created.id, "failed");
      expect(failed.failedReason).toContain("easymq:cancelled");
      expect(failed.attemptsMade).toBe(2);
    } finally {
      system.fake.setBehavior({ kind: "block" });
    }
  });

  it("cancellation is idempotent", async () => {
    const queue = `q-${prefix}-cancel-idem`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    await waitForJob(system.jobService, queue, created.id, "active");
    // Concurrent repeats are all safe: each either succeeds or honestly
    // reports that the first request already finished the job.
    const outcomes = await Promise.allSettled([
      system.jobService.cancelJob(queue, created.id),
      system.jobService.cancelJob(queue, created.id),
    ]);
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        expect(outcome.reason).toMatchObject({ code: "JOB_NOT_ACTIVE" });
      }
    }
    await waitForJob(system.jobService, queue, created.id, "failed");
    // Request after terminal state reports the race honestly.
    await expect(system.jobService.cancelJob(queue, created.id)).rejects.toMatchObject({
      code: "JOB_NOT_ACTIVE",
    });
  });

  it("keeps completed results and cannot touch retries or recreated IDs", async () => {
    system.fake.setBehavior({ kind: "success" });
    try {
      const queue = `q-${prefix}-race`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        jobId: `race-${prefix}`,
      });
      await waitForJob(system.jobService, queue, created.id, "completed");
      // Completion wins: cancelling afterwards is honestly rejected...
      await expect(system.jobService.cancelJob(queue, created.id)).rejects.toMatchObject({
        code: "JOB_NOT_ACTIVE",
      });
      // ...and manual retry of the completed job runs normally.
      await system.jobService.retryJob(queue, created.id);
      await waitForJob(system.jobService, queue, created.id, "completed");

      // Remove + recreate with the same ID: new generation, new timestamp.
      await system.jobService.removeJob(queue, created.id);
      const staleTimestamp = created.timestampMs;
      const recreated = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        jobId: `race-${prefix}`,
      });
      expect(recreated.timestampMs).not.toBe(staleTimestamp);
      // A stale marker naming the OLD generation cannot affect the new job.
      const shared = system.connections.getShared();
      await shared.set(
        markerKey(prefix, queue, recreated.id),
        JSON.stringify({
          queue,
          jobId: recreated.id,
          jobTimestampMs: staleTimestamp,
          attemptsMade: 0,
          requestedAtMs: Date.now(),
        }),
        "EX",
        60,
      );
      const done = await waitForJob(system.jobService, queue, recreated.id, "completed");
      expect(done.failedReason).toBeNull();
    } finally {
      system.fake.setBehavior({ kind: "block" });
    }
  });

  it("reaches the active worker across multiple instances", async () => {
    const second = new WorkerManager({
      connections: system.connections,
      catalog: system.catalog,
      cancellation: system.cancellation,
      processor: system.processor,
      options: { concurrency: 5, prefix },
    });
    await second.start();
    try {
      const queue = `q-${prefix}-multi`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      });
      await waitForJob(system.jobService, queue, created.id, "active");
      // Both managers hold a subscription; only the holder can cancel.
      await system.jobService.cancelJob(queue, created.id);
      const failed = await waitForJob(system.jobService, queue, created.id, "failed");
      expect(failed.failedReason).toContain("easymq:cancelled");
    } finally {
      await second.stop();
      // second.stop() tears down the shared catalog subscription; bring
      // discovery back for the still-running first manager.
      await system.catalog.start();
    }
  });

  it("recovers a missed notification after subscriber reconnect", async () => {
    const subscriber = system.cancellation.getSubscriber();
    expect(subscriber).toBeDefined();
    if (!subscriber) throw new Error("no cancellation subscriber");
    subscriber.disconnect();
    await waitFor(() => subscriber.status === "end" || subscriber.status === "close", {
      timeoutMs: 5000,
      label: "subscriber to drop",
    });

    const queue = `q-${prefix}-reconnect`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    await waitForJob(system.jobService, queue, created.id, "active");
    // Prompt is published while this instance is deaf.
    await system.jobService.cancelJob(queue, created.id);
    // Give any stray delivery a chance — the job must stay active,
    // proving the prompt was really missed and not just slow.
    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect((await system.jobService.getJob(queue, created.id)).state).toBe("active");

    // Reconnect: the ready handler reconciles markers against active jobs.
    await subscriber.connect();
    await waitFor(() => subscriber.status === "ready", {
      timeoutMs: 15_000,
      label: "subscriber to reconnect",
    });
    const failed = await waitForJob(system.jobService, queue, created.id, "failed");
    expect(failed.failedReason).toContain("easymq:cancelled");
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
