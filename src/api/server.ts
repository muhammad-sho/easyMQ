import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import fastify, { type FastifyInstance, type FastifyTypeProviderDefault } from "fastify";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import { ApiError } from "./errors.js";
import type { AppConfig } from "../config/schema.js";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { HealthService } from "../health/health-service.js";
import type { QueueService } from "../queues/queue-service.js";
import type { JobService } from "../jobs/job-service.js";
import type { ScheduleService } from "../jobs/schedule-service.js";
import { registerHealthRoutes } from "./routes/health.js";
import { registerJobRoutes } from "./routes/jobs.js";
import { registerQueueRoutes } from "./routes/queues.js";
import { registerScheduleRoutes } from "./routes/schedules.js";

export interface ApiServices {
  config: AppConfig;
  logger: Logger;
  queueService: QueueService;
  jobService: JobService;
  scheduleService: ScheduleService;
  healthService: HealthService;
}

/** Fastify instance type bound to the pino logger easyMQ uses. */
export type AppInstance = FastifyInstance<
  Server<typeof IncomingMessage, typeof ServerResponse>,
  IncomingMessage,
  ServerResponse<IncomingMessage>,
  Logger,
  FastifyTypeProviderDefault
>;

/** Health probes stay unauthenticated so orchestrators can check them. */
const PUBLIC_PATHS = new Set(["/health/live", "/health/ready"]);

function isAuthorized(
  request: { url: string; headers: Record<string, string | string[] | undefined> },
  config: AppConfig,
): boolean {
  const path = request.url.split("?", 1)[0] ?? request.url;
  if (PUBLIC_PATHS.has(path)) return true;
  if (config.authDisabled) return true;
  const header = request.headers["authorization"];
  if (typeof header !== "string" || config.apiToken === undefined) return false;
  const [scheme, token] = header.split(" ", 2);
  if (scheme?.toLowerCase() !== "bearer" || !token) return false;
  const expected = Buffer.from(`Bearer ${config.apiToken}`);
  const actual = Buffer.from(header);
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

/**
 * Build the Fastify application. Routes are thin: validation happens in
 * Zod schemas, business logic in application services.
 */
export async function buildApp(services: ApiServices): Promise<AppInstance> {
  const { config, logger } = services;
  // Ensure server keep-alive sockets cannot outlive a shutdown request.
  const app = fastify({ loggerInstance: logger, forceCloseConnections: true });

  await app.register(helmet);
  await app.register(rateLimit, { max: 1000, timeWindow: "1 minute" });

  app.addHook("onRequest", async (request, reply) => {
    if (!isAuthorized(request, config)) {
      return reply
        .status(401)
        .send(new ApiError("UNAUTHENTICATED", "Missing or invalid Bearer token.").toBody());
    }
  });

  app.setErrorHandler((err, request, reply) => {
    if (err instanceof ApiError) {
      if (err.statusCode >= 500) {
        request.log.error({ err, code: err.code }, "Request failed");
      }
      return reply.status(err.statusCode).send(err.toBody());
    }
    if (typeof err === "object" && err !== null && "validation" in err) {
      return reply.status(400).send(
        ApiError.validation("Invalid request.", {
          issues: (err as { validation?: unknown }).validation,
        }).toBody(),
      );
    }
    request.log.error({ err }, "Unhandled request error");
    const statusCode =
      typeof (err as { statusCode?: unknown }).statusCode === "number"
        ? (err as { statusCode: number }).statusCode
        : 500;
    return reply
      .status(statusCode >= 400 && statusCode < 600 ? statusCode : 500)
      .send(
        new ApiError(
          statusCode === 503 ? "SERVICE_UNAVAILABLE" : "INTERNAL_ERROR",
          statusCode >= 500 ? "Internal server error." : "Request failed.",
        ).toBody(),
      );
  });

  app.setNotFoundHandler((request, reply) => {
    return reply.status(404).send(
      new ApiError("NOT_FOUND", `Route '${request.url}' not found.`, {
        resource: { type: "route", id: request.url },
      }).toBody(),
    );
  });

  registerHealthRoutes(app, services);
  registerQueueRoutes(app, services);
  registerJobRoutes(app, services);
  registerScheduleRoutes(app, services);

  return app;
}
