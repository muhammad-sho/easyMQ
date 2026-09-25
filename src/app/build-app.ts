import type { AppInstance } from "../api/server.js";
import { buildApp, type ApiServices } from "../api/server.js";
import { BrokerService } from "../broker/broker.js";
import { QueueSweeper } from "../broker/sweeper.js";
import { SubscriptionManager } from "../broker/subscriptions.js";
import { resolveApiAuth } from "../config/api-token.js";
import type { AppConfig } from "../config/schema.js";
import { HealthService } from "../health/health-service.js";
import { createLogger, type Logger } from "../infrastructure/logging/logger.js";
import { RedisConnectionManager } from "../infrastructure/redis/connection-manager.js";

export interface BuiltSystem {
  config: AppConfig;
  logger: Logger;
  connections: RedisConnectionManager;
  broker: BrokerService;
  sweeper: QueueSweeper;
  subscriptions: SubscriptionManager;
  healthService: HealthService;
  fastifyApp: AppInstance;
  /** Ordered shutdown: HTTP -> sweeper -> subscriptions -> Redis. */
  close: () => Promise<void>;
}

/** Compose the full system: HTTP API (broker) + background sweeper. */
export async function buildSystem(input: AppConfig): Promise<BuiltSystem> {
  const logger = createLogger({
    level: input.logLevel,
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

  const broker = new BrokerService(shared, {
    prefix: config.redisKeyPrefix,
    defaultVisibilityTimeoutMs: config.defaultVisibilityTimeoutMs,
    defaultPrefetch: config.defaultPrefetch,
    maxConsumeCount: config.maxConsumeCount,
    maxMessageBytes: config.maxMessageBytes,
  });
  const sweeper = new QueueSweeper(broker, config.sweeperIntervalMs, logger);
  const subscriptions = new SubscriptionManager(broker, logger);
  subscriptions.attach();
  const healthService = new HealthService(logger);
  healthService.addCheck({
    name: "redis",
    check: () => shared.ping().then(() => undefined),
  });

  const services: ApiServices = { config, logger, broker, healthService, subscriptions };
  const fastifyApp = await buildApp(services);

  async function close(): Promise<void> {
    // 1. Stop accepting new HTTP requests.
    try {
      await fastifyApp.close();
    } catch (err) {
      logger.warn({ err }, "Error closing HTTP server");
    }
    // 2. Stop the background sweeper.
    try {
      sweeper.stop();
    } catch (err) {
      logger.warn({ err }, "Error stopping queue sweeper");
    }
    // 3. Cancel persistent consumers (their pending messages requeue).
    try {
      await subscriptions.shutdown();
    } catch (err) {
      logger.warn({ err }, "Error shutting down subscriptions");
    }
    // 4. Release Redis connections.
    await connections.closeAll();
  }

  return {
    config,
    logger,
    connections,
    broker,
    sweeper,
    subscriptions,
    healthService,
    fastifyApp,
    close,
  };
}
