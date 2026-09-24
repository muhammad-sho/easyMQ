import type { AppConfig } from "../config/schema.js";
import { buildSystem, type BuiltSystem } from "./build-app.js";

function withTimeout(promise: Promise<void>, ms: number): Promise<void> {
  if (ms <= 0) return promise;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<void>((_, reject) => {
    timer = setTimeout(() => reject(new Error("shutdown deadline exceeded")), ms);
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

/**
 * Start the system for the configured role and wait for SIGTERM/SIGINT.
 *
 * Shutdown order: stop HTTP -> stop workers fetching new jobs -> allow
 * active jobs to finish within the deadline -> close QueueEvents ->
 * close subscriptions -> release BullMQ/Redis resources -> exit.
 * BullMQ recovers unfinished work after a crash; no custom recovery.
 */
export async function run(config: AppConfig): Promise<void> {
  const system: BuiltSystem = await buildSystem(config);
  const { logger } = system;

  const shutdownRequested = new Promise<string>((resolve) => {
    for (const signal of ["SIGTERM", "SIGINT"] as const) {
      process.once(signal, () => resolve(signal));
    }
  });

  if (system.fastifyApp) {
    await system.fastifyApp.listen({ host: config.apiHost, port: config.apiPort });
    logger.info(
      { event: "api-listening", host: config.apiHost, port: config.apiPort },
      "easyMQ API listening",
    );
  }
  if (system.workerManager) {
    await system.workerManager.start();
  }
  logger.info({ event: "started", role: config.appRole }, "easyMQ started");

  const signal = await shutdownRequested;
  logger.info({ event: "shutdown-signal", signal }, "Shutdown requested");

  try {
    await withTimeout(system.close(), config.shutdownTimeoutMs);
    logger.info({ event: "shutdown-complete" }, "easyMQ shut down cleanly");
  } catch (err) {
    logger.warn(
      { err, event: "shutdown-timeout", deadlineMs: config.shutdownTimeoutMs },
      "Shutdown deadline exceeded — terminating with work potentially unfinished",
    );
  }
}
