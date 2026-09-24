import type { AppInstance } from "../api/server.js";
import { buildApp, type ApiServices } from "../api/server.js";
import { resolveApiAuth } from "../config/api-token.js";
import type { AppConfig } from "../config/schema.js";
import { HttpExecutor } from "../executors/http-executor.js";
import { HealthService } from "../health/health-service.js";
import { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";
import { createLogger, type Logger } from "../infrastructure/logging/logger.js";
import { RedisConnectionManager } from "../infrastructure/redis/connection-manager.js";
import { JobService } from "../jobs/job-service.js";
import { ScheduleService } from "../jobs/schedule-service.js";
import { QueueCatalog } from "../queues/queue-catalog.js";
import { QueueService } from "../queues/queue-service.js";
import { CancellationCoordinator } from "../workers/cancellation.js";
import { JobProcessor } from "../workers/job-processor.js";
import { WorkerManager } from "../workers/worker-manager.js";

export interface BuiltSystem {
  config: AppConfig;
  logger: Logger;
  connections: RedisConnectionManager;
  healthService: HealthService;
  fastifyApp: AppInstance | undefined;
  workerManager: WorkerManager | undefined;
  /** Ordered shutdown: HTTP -> workers -> subscriptions -> queues -> Redis. */
  close: () => Promise<void>;
}

/**
 * Compose the full system for the configured role.
 * API role creates no BullMQ Workers and no subscriptions;
 * worker role creates no HTTP server.
 */
export async function buildSystem(input: AppConfig): Promise<BuiltSystem> {
  const logger = createLogger({
    level: input.logLevel,
    role: input.appRole,
    pretty: input.logPretty,
  });
  const connections = new RedisConnectionManager(input.redisUrl, logger);
  const shared = connections.getShared();
  let config = input;
  try {
    await connections.waitUntilReady();
    // Zero-config deployments omit API_TOKEN; resolve a secure per-deployment
    // token (env override > Redis-persisted > generate once) before wiring auth.
    config = (await resolveApiAuth(config, shared, logger)).config;
  } catch (err) {
    // ioredis otherwise keeps retry timers alive after a failed startup.
    connections.disconnectAll();
    throw err;
  }

  const catalog = new QueueCatalog(
    shared,
    () => connections.createDedicated("catalog-sub"),
    { keyPrefix: config.redisKeyPrefix },
    logger,
  );
  const queueFactory = new QueueFactory(
    connections,
    {
      prefix: config.redisKeyPrefix,
      defaultJobOptions: {
        attempts: config.defaultAttempts,
        backoff: {
          type: config.defaultBackoffType,
          delay: config.defaultBackoffDelayMs,
        },
        removeOnComplete: { count: config.defaultRemoveOnCompleteCount },
        removeOnFail: { count: config.defaultRemoveOnFailCount },
      },
    },
    logger,
  );
  const cancellation = new CancellationCoordinator(
    shared,
    () => connections.createDedicated("cancel-sub"),
    queueFactory,
    {
      keyPrefix: config.redisKeyPrefix,
      ttlSeconds: config.cancellationTtlSeconds,
    },
    logger,
  );

  const queueService = new QueueService(queueFactory, catalog);
  const jobService = new JobService(queueFactory, catalog, config, cancellation);
  const scheduleService = new ScheduleService(queueFactory, catalog, config);
  const healthService = new HealthService(config.appRole, logger);
  healthService.addCheck({
    name: "redis",
    check: () => shared.ping().then(() => undefined),
  });

  const servesApi = config.appRole === "api" || config.appRole === "both";
  const servesWorker = config.appRole === "worker" || config.appRole === "both";

  let fastifyApp: AppInstance | undefined;
  if (servesApi) {
    const services: ApiServices = {
      config,
      logger,
      queueService,
      jobService,
      scheduleService,
      healthService,
    };
    fastifyApp = await buildApp(services);
  }

  let workerManager: WorkerManager | undefined;
  if (servesWorker) {
    const processor = new JobProcessor(
      {
        executors: [
          new HttpExecutor(
            {
              timeoutMs: config.httpTimeoutMs,
              maxResponseBytes: config.httpMaxResponseBytes,
              maxRedirects: config.httpMaxRedirects,
              allowPrivateNetwork: config.httpAllowPrivateNetwork,
            },
            logger,
          ),
        ],
      },
      cancellation,
      logger,
    );
    workerManager = new WorkerManager({
      connections,
      catalog,
      cancellation,
      processor,
      options: {
        concurrency: config.workerConcurrency,
        prefix: config.redisKeyPrefix,
      },
      logger,
    });
  }

  async function close(): Promise<void> {
    // 1. Stop accepting new HTTP requests.
    if (fastifyApp) {
      try {
        await fastifyApp.close();
      } catch (err) {
        logger.warn({ err }, "Error closing HTTP server");
      }
    }
    // 2-4. Workers (graceful close bounded by the shutdown deadline,
    // then abort + force-close), cancellation + catalog subscriptions.
    if (workerManager) {
      try {
        await workerManager.stop({ shutdownTimeoutMs: config.shutdownTimeoutMs });
      } catch (err) {
        logger.warn({ err }, "Error stopping worker manager");
      }
    }
    try {
      await catalog.stop();
    } catch {
      // already stopped by the worker manager — ignore
    }
    // 5. BullMQ Queue objects.
    try {
      await queueFactory.closeAll();
    } catch (err) {
      logger.warn({ err }, "Error closing queues");
    }
    // 6. Directly-owned Redis connections.
    await connections.closeAll();
  }

  return {
    config,
    logger,
    connections,
    healthService,
    fastifyApp,
    workerManager,
    close,
  };
}
