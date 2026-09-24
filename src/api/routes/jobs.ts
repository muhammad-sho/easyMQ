import type { AppInstance } from "../server.js";
import { ApiError } from "../errors.js";
import type { EasyMQJobState } from "../../jobs/job-types.js";
import { changeDelaySchema, createJobSchema } from "../schemas/jobs.js";
import { jobParamsSchema, queueParamsSchema } from "../schemas/jobs.js";
import { paginationQuerySchema } from "../schemas/common.js";
import { normalizeListParam, parseWith } from "./helpers.js";
import type { ApiServices } from "../server.js";

const VALID_STATES: EasyMQJobState[] = [
  "waiting",
  "active",
  "completed",
  "failed",
  "delayed",
  "prioritized",
  "waiting-children",
  "unknown",
];

export function registerJobRoutes(app: AppInstance, services: ApiServices): void {
  const { jobService } = services;

  app.post("/jobs", async (request, reply) => {
    const input = parseWith(createJobSchema, request.body, "request body");
    const job = await jobService.createJob({
      queue: input.queue,
      ...(input.name !== undefined ? { name: input.name } : {}),
      ...(input.payload !== undefined ? { payload: input.payload } : {}),
      execution: input.execution,
      ...(input.jobId !== undefined ? { jobId: input.jobId } : {}),
      ...(input.delayMs !== undefined ? { delayMs: input.delayMs } : {}),
      ...(input.attempts !== undefined ? { attempts: input.attempts } : {}),
      ...(input.backoff !== undefined ? { backoff: input.backoff } : {}),
      ...(input.priority !== undefined ? { priority: input.priority } : {}),
      ...(input.lifo !== undefined ? { lifo: input.lifo } : {}),
      ...(input.deduplication !== undefined ? { deduplication: input.deduplication } : {}),
      ...(input.debounce !== undefined ? { debounce: input.debounce } : {}),
      ...(input.removeOnComplete !== undefined
        ? { removeOnComplete: input.removeOnComplete }
        : {}),
      ...(input.removeOnFail !== undefined ? { removeOnFail: input.removeOnFail } : {}),
    });
    return reply.status(201).send(job);
  });

  app.get("/queues/:queue/jobs", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const pagination = parseWith(paginationQuerySchema, request.query, "query string");
    const query = (request.query ?? {}) as Record<string, unknown>;
    const states = normalizeListParam(query["state"]);
    for (const state of states) {
      if (!(VALID_STATES as string[]).includes(state)) {
        throw ApiError.validation(
          `Invalid job state '${state}'. Valid states: ${VALID_STATES.join(", ")}.`,
        );
      }
    }
    return jobService.listJobs({
      queue: params.queue,
      ...(states.length > 0 ? { states: states as EasyMQJobState[] } : {}),
      ...(pagination.offset !== undefined ? { offset: pagination.offset } : {}),
      ...(pagination.limit !== undefined ? { limit: pagination.limit } : {}),
      ...(pagination.asc !== undefined ? { asc: pagination.asc } : {}),
    });
  });

  app.get("/queues/:queue/jobs/:id", async (request) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    return jobService.getJob(params.queue, params.id);
  });

  app.delete("/queues/:queue/jobs/:id", async (request, reply) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    await jobService.removeJob(params.queue, params.id);
    return reply.status(204).send();
  });

  app.post("/queues/:queue/jobs/:id/retry", async (request) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    return jobService.retryJob(params.queue, params.id);
  });

  app.post("/queues/:queue/jobs/:id/promote", async (request) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    return jobService.promoteJob(params.queue, params.id);
  });

  app.patch("/queues/:queue/jobs/:id/delay", async (request) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    const body = parseWith(changeDelaySchema, request.body, "request body");
    return jobService.changeJobDelay(params.queue, params.id, body.delayMs);
  });

  app.post("/queues/:queue/jobs/:id/cancel", async (request, reply) => {
    const params = parseWith(jobParamsSchema, request.params, "path parameters");
    const job = await jobService.cancelJob(params.queue, params.id);
    return reply.status(202).send(job);
  });
}
