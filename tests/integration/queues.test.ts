import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ApiError } from "../../src/api/errors.js";
import { buildTestSystem, closeTestSystem, uniquePrefix, type TestSystem } from "./helpers.js";

describe("queue registration and control", () => {
  const prefix = uniquePrefix("queues");
  let system: TestSystem;
  const queue = `q-${prefix}-main`;

  beforeAll(async () => {
    system = buildTestSystem(prefix);
    await system.connections.waitUntilReady();
  });

  afterAll(async () => {
    await closeTestSystem(system);
  });

  it("registers queues idempotently and lists them sorted", async () => {
    expect(await system.catalog.register(queue)).toBe(true);
    expect(await system.catalog.register(queue)).toBe(false);
    const other = `q-${prefix}-aaa`;
    await system.catalog.register(other);
    const names = await system.catalog.list();
    expect(names).toContain(queue);
    expect(names).toContain(other);
    expect([...names].sort()).toEqual(names);
    expect(await system.queueService.listQueues()).toEqual(names);
  });

  it("rejects invalid queue names", async () => {
    await expect(system.catalog.register("")).rejects.toBeInstanceOf(ApiError);
    await expect(
      system.jobService.createJob({
        queue: "",
        payload: null,
        execution: { type: "http", url: "https://example.com/" },
      }),
    ).rejects.toBeInstanceOf(ApiError);
  });

  it("pauses and resumes queues, reporting counts", async () => {
    await system.jobService.createJob({
      queue,
      payload: { n: 1 },
      execution: { type: "http", url: "https://example.com/" },
      delayMs: 60_000,
    });
    const info = await system.queueService.getQueue(queue);
    expect(info.name).toBe(queue);
    expect(info.isPaused).toBe(false);
    expect(info.counts["delayed"]).toBe(1);

    const paused = await system.queueService.pauseQueue(queue);
    expect(paused.isPaused).toBe(true);
    const resumed = await system.queueService.resumeQueue(queue);
    expect(resumed.isPaused).toBe(false);

    const counts = await system.queueService.getJobCounts(queue);
    expect(counts["delayed"]).toBe(1);
  });

  it("returns NOT_FOUND for unknown queues", async () => {
    await expect(system.queueService.getQueue(`missing-${prefix}`)).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
  });
});
