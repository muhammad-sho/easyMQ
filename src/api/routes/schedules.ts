import type { AppInstance } from "../server.js";
import { scheduleParamsSchema, upsertScheduleSchema } from "../schemas/schedules.js";
import { queueParamsSchema } from "../schemas/jobs.js";
import { paginationQuerySchema } from "../schemas/common.js";
import { parseWith } from "./helpers.js";
import type { ApiServices } from "../server.js";

export function registerScheduleRoutes(app: AppInstance, services: ApiServices): void {
  const { scheduleService } = services;

  app.post("/schedules", async (request) => {
    const input = parseWith(upsertScheduleSchema, request.body, "request body");
    return scheduleService.upsertSchedule({
      id: input.id,
      queue: input.queue,
      ...(input.name !== undefined ? { name: input.name } : {}),
      ...(input.pattern !== undefined ? { pattern: input.pattern } : {}),
      ...(input.everyMs !== undefined ? { everyMs: input.everyMs } : {}),
      ...(input.timezone !== undefined ? { timezone: input.timezone } : {}),
      ...(input.startDateMs !== undefined ? { startDateMs: input.startDateMs } : {}),
      ...(input.endDateMs !== undefined ? { endDateMs: input.endDateMs } : {}),
      ...(input.limit !== undefined ? { limit: input.limit } : {}),
      ...(input.payload !== undefined ? { payload: input.payload } : {}),
      execution: input.execution,
      ...(input.attempts !== undefined ? { attempts: input.attempts } : {}),
      ...(input.backoff !== undefined ? { backoff: input.backoff } : {}),
      ...(input.priority !== undefined ? { priority: input.priority } : {}),
      ...(input.removeOnComplete !== undefined
        ? { removeOnComplete: input.removeOnComplete }
        : {}),
      ...(input.removeOnFail !== undefined ? { removeOnFail: input.removeOnFail } : {}),
    });
  });

  app.get("/queues/:queue/schedules", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const pagination = parseWith(paginationQuerySchema, request.query, "query string");
    return scheduleService.listSchedules(
      params.queue,
      pagination.offset,
      pagination.limit,
    );
  });

  app.get("/queues/:queue/schedules/:id", async (request) => {
    const params = parseWith(scheduleParamsSchema, request.params, "path parameters");
    return scheduleService.getSchedule(params.queue, params.id);
  });

  app.delete("/queues/:queue/schedules/:id", async (request, reply) => {
    const params = parseWith(scheduleParamsSchema, request.params, "path parameters");
    await scheduleService.removeSchedule(params.queue, params.id);
    return reply.status(204).send();
  });
}
