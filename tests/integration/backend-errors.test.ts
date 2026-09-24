import { describe, expect, it } from "vitest";
import { buildTestSystem, closeTestSystem, uniquePrefix } from "./helpers.js";

const DEAD_REDIS_URL = "redis://127.0.0.1:6399";

describe("backend error classification", () => {
  const prefix = uniquePrefix("backend-errors");

  it("maps an unreachable backend to 503 without internals", async () => {
    const system = buildTestSystem(prefix, { REDIS_URL: DEAD_REDIS_URL });
    try {
      const err = await system.jobService
        .createJob({
          queue: `q-${prefix}-dead`,
          payload: null,
          execution: { type: "http", url: "https://example.com/hook" },
        })
        .catch((e: unknown) => e);
      expect(err).toMatchObject({ code: "SERVICE_UNAVAILABLE" });
      const body = JSON.stringify(err);
      expect(body).not.toContain("6399");
      expect(body).not.toContain("ECONNREFUSED");
    } finally {
      await closeTestSystem(system);
    }
  });

  it("reports deterministic conflicts for non-delayed promote/delay", async () => {
    const system = buildTestSystem(`${prefix}-conflict`);
    await system.connections.waitUntilReady();
    await system.manager.start();
    try {
      const queue = `q-${prefix}-conflict`;
      const created = await system.jobService.createJob({
        queue,
        payload: null,
        execution: { type: "http", url: "https://example.com/hook" },
      });
      await expect(system.jobService.promoteJob(queue, created.id)).rejects.toMatchObject({
        code: "CONFLICT",
      });
      await expect(system.jobService.changeJobDelay(queue, created.id, 1000)).rejects.toMatchObject(
        { code: "CONFLICT" },
      );
    } finally {
      await closeTestSystem(system);
    }
  });
});
