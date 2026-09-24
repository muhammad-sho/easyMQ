import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  buildTestSystem,
  closeTestSystem,
  uniquePrefix,
  waitFor,
  waitForJob,
  type TestSystem,
} from "./helpers.js";
import { WorkerManager } from "../../src/workers/worker-manager.js";

describe("worker discovery and distribution", () => {
  const prefix = uniquePrefix("workers");
  let system: TestSystem;

  beforeAll(async () => {
    system = buildTestSystem(prefix);
    await system.connections.waitUntilReady();
  });

  afterAll(async () => {
    await closeTestSystem(system);
  });

  it("discovers pre-registered queues on startup", async () => {
    const queue = `q-${prefix}-pre`;
    await system.catalog.register(queue);
    await system.manager.start();
    expect(system.manager.queueNames()).toContain(queue);
    // No per-queue QueueEvents consumers exist: one managed queue costs
    // exactly one worker connection (shared + cancel/catalog
    // subscriptions + worker = 4 tracked clients, no `events:` client).
    expect(system.connections.size).toBe(4);
  });

  it("creates workers for late registrations", async () => {
    const queue = `q-${prefix}-late`;
    const created = await system.jobService.createJob({
      queue,
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    await waitFor(() => system.manager.queueNames().includes(queue), {
      label: `worker for ${queue}`,
    });
    await waitForJob(system.jobService, queue, created.id, "completed");
  });

  it("distributes work across multiple instances on the same queue", async () => {
    const queue = `q-${prefix}-shared`;
    const second = new WorkerManager({
      connections: system.connections,
      catalog: system.catalog,
      cancellation: system.cancellation,
      processor: system.processor,
      options: { concurrency: 5, prefix },
    });
    await second.start();
    try {
      const ids: string[] = [];
      for (let n = 0; n < 10; n++) {
        const created = await system.jobService.createJob({
          queue,
          payload: { n },
          execution: { type: "http", url: "https://example.com/hook" },
        });
        ids.push(created.id);
      }
      for (const id of ids) {
        await waitForJob(system.jobService, queue, id, "completed", 30_000);
      }
      expect(system.manager.queueNames()).toContain(queue);
      expect(second.queueNames()).toContain(queue);
    } finally {
      await second.stop();
      await system.catalog.start();
    }
  });

  it("discovers newly submitted schedules on every instance", async () => {
    const second = new WorkerManager({
      connections: system.connections,
      catalog: system.catalog,
      cancellation: system.cancellation,
      processor: system.processor,
      options: { concurrency: 5, prefix },
    });
    await second.start();
    try {
      const queue = `q-${prefix}-sched-discovery`;
      await system.scheduleService.upsertSchedule({
        id: `disc-${prefix}`,
        queue,
        everyMs: 3_600_000,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      });
      await waitFor(() => system.manager.queueNames().includes(queue), {
        label: `worker for ${queue} on first manager`,
      });
      await waitFor(() => second.queueNames().includes(queue), {
        label: `worker for ${queue} on second manager`,
      });
    } finally {
      await second.stop();
      await system.catalog.start();
    }
  });
});
