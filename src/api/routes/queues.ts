import type { AppInstance } from "../server.js";
import { queueParamsSchema } from "../schemas/jobs.js";
import { parseWith } from "./helpers.js";
import type { ApiServices } from "../server.js";

export function registerQueueRoutes(app: AppInstance, services: ApiServices): void {
  const { queueService } = services;

  app.get("/queues", async () => {
    const queues = await queueService.listQueues();
    return { queues };
  });

  app.get("/queues/:queue", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    return queueService.getQueue(params.queue);
  });

  app.post("/queues/:queue/pause", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    return queueService.pauseQueue(params.queue);
  });

  app.post("/queues/:queue/resume", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    return queueService.resumeQueue(params.queue);
  });

  app.get("/queues/:queue/counts", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const counts = await queueService.getJobCounts(params.queue);
    return { queue: params.queue, counts };
  });
}
