import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  buildTestSystem,
  closeTestSystem,
  uniquePrefix,
  waitFor,
  type TestSystem,
} from "./helpers.js";

describe("recurring schedules", () => {
  const prefix = uniquePrefix("sched");
  let system: TestSystem;
  const queue = `q-${prefix}-sched`;

  beforeAll(async () => {
    system = buildTestSystem(prefix);
    await system.connections.waitUntilReady();
    await system.manager.start();
  });

  afterAll(async () => {
    await closeTestSystem(system);
  });

  it("creates, gets, lists and removes an interval schedule", async () => {
    const created = await system.scheduleService.upsertSchedule({
      id: `every-${prefix}`,
      queue,
      everyMs: 1000,
      payload: { tick: true },
      execution: { type: "http", url: "https://example.com/hook" },
    });
    expect(created.queue).toBe(queue);
    expect(created.everyMs).toBe(1000);
    expect(created.pattern).toBeNull();

    const fetched = await system.scheduleService.getSchedule(queue, `every-${prefix}`);
    expect(fetched.id).toBe(`every-${prefix}`);

    const page = await system.scheduleService.listSchedules(queue);
    expect(page.schedules.map((s) => s.id)).toContain(`every-${prefix}`);
    expect(page.nextOffset).toBeNull();

    await system.scheduleService.removeSchedule(queue, `every-${prefix}`);
    await expect(
      system.scheduleService.getSchedule(queue, `every-${prefix}`),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("supports cron schedules with timezones", async () => {
    const id = `cron-${prefix}`;
    const created = await system.scheduleService.upsertSchedule({
      id,
      queue,
      pattern: "*/5 * * * *",
      timezone: "Europe/Berlin",
      payload: null,
      execution: { type: "http", url: "https://example.com/hook" },
    });
    expect(created.pattern).toBe("*/5 * * * *");
    expect(created.timezone).toBe("Europe/Berlin");
    await system.scheduleService.removeSchedule(queue, id);
  });

  it("validates schedule input", async () => {
    await expect(
      system.scheduleService.upsertSchedule({
        id: `bad-${prefix}`,
        queue,
        pattern: "* * * * *",
        everyMs: 1000,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION_ERROR" });
    await expect(
      system.scheduleService.upsertSchedule({
        id: `bad-tz-${prefix}`,
        queue,
        pattern: "* * * * *",
        timezone: "Mars/Olympus",
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      }),
    ).rejects.toMatchObject({ code: "VALIDATION_ERROR" });
  });

  it("fires scheduled jobs through workers", async () => {
    const id = `fire-${prefix}`;
    await system.scheduleService.upsertSchedule({
      id,
      queue,
      everyMs: 1000,
      payload: { scheduled: true },
      execution: { type: "http", url: "https://example.com/hook" },
    });
    try {
      await waitFor(
        async () => {
          const page = await system.jobService.listJobs({ queue, states: ["completed"] });
          return page.jobs.length > 0;
        },
        { timeoutMs: 30_000, label: "scheduled job to complete" },
      );
    } finally {
      await system.scheduleService.removeSchedule(queue, id);
    }
  });
});
