import { describe, expect, it } from "vitest";
import { buildTestSystem, closeTestSystem, uniquePrefix, waitForJob } from "./helpers.js";

describe("graceful shutdown with deadline", () => {
  const prefix = uniquePrefix("shutdown");

  it("drains a cooperative executor before the deadline", async () => {
    const system = buildTestSystem(`${prefix}-drain`);
    system.fake.setBehavior({ kind: "slow", ms: 400 });
    await system.connections.waitUntilReady();
    await system.manager.start();
    try {
      const queue = `q-${prefix}-drain`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      });
      await waitForJob(system.jobService, queue, created.id, "active");
      const started = Date.now();
      await system.manager.stop({ shutdownTimeoutMs: 10_000 });
      expect(Date.now() - started).toBeLessThan(10_000);
      // The cooperative attempt ran to completion instead of being killed.
      const done = await system.jobService.getJob(queue, created.id);
      expect(done.state).toBe("completed");
    } finally {
      await closeTestSystem(system);
    }
  });

  it("bounds stop time for a non-cooperative executor and releases resources", async () => {
    const system = buildTestSystem(`${prefix}-force`);
    system.fake.setBehavior({ kind: "never" });
    await system.connections.waitUntilReady();
    await system.manager.start();
    try {
      const queue = `q-${prefix}-force`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
        attempts: 1,
      });
      await waitForJob(system.jobService, queue, created.id, "active");
      const started = Date.now();
      await system.manager.stop({ shutdownTimeoutMs: 800, forceGraceMs: 800 });
      const elapsed = Date.now() - started;
      expect(elapsed).toBeLessThan(10_000);
      // A second stop is safe (idempotent cleanup).
      await system.manager.stop({ shutdownTimeoutMs: 800, forceGraceMs: 200 });
      // The stuck attempt did not complete; the job is left for recovery.
      const left = await system.jobService.getJob(queue, created.id);
      expect(left.state).not.toBe("completed");
    } finally {
      await closeTestSystem(system);
    }
    expect(system.connections.size).toBe(0);
  });

  it("lets another worker recover interrupted work", async () => {
    const recoveringPrefix = `${prefix}-recover`;
    const first = buildTestSystem(
      recoveringPrefix,
      {},
      { lockDurationMs: 2000, stalledIntervalMs: 1000 },
    );
    first.fake.setBehavior({ kind: "never" });
    await first.connections.waitUntilReady();
    await first.manager.start();
    const queue = `q-${prefix}-recover`;
    const created = await first.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
      attempts: 5,
    });
    await waitForJob(first.jobService, queue, created.id, "active");
    // Force-stop while the attempt is stuck, then simulate process death by
    // closing the first system's connections: lock renewal stops, the lock
    // expires, and BullMQ stalled-job recovery hands the work to the next
    // worker. No custom recovery is involved.
    await first.manager.stop({ shutdownTimeoutMs: 500, forceGraceMs: 500 });
    await closeTestSystem(first);

    const second = buildTestSystem(
      recoveringPrefix,
      {},
      { lockDurationMs: 2000, stalledIntervalMs: 1000 },
    );
    await second.connections.waitUntilReady();
    await second.manager.start();
    try {
      const done = await waitForJob(second.jobService, queue, created.id, "completed", 45_000);
      expect(done.returnValue).toMatchObject({ statusCode: 200 });
    } finally {
      await closeTestSystem(second);
    }
  });
});
