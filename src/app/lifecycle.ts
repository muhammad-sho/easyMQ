import type { AppConfig } from "../config/schema.js";
import { buildSystem, type BuiltSystem } from "./build-app.js";

/**
 * Start the system for the configured role and wait for SIGTERM/SIGINT.
 *
 * Shutdown order: stop HTTP -> stop workers fetching new jobs -> allow
 * active jobs to finish within the deadline (then abort in-flight attempts
 * and force-close) -> close subscriptions -> release BullMQ/Redis resources
 * -> exit. BullMQ recovers unfinished work after a crash; no custom recovery.
 * WorkerManager owns the worker deadline and must be allowed to finish its
 * force-close/resource cleanup rather than being abandoned by Promise.race.
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

  await system.close();
  logger.info({ event: "shutdown-complete" }, "easyMQ shut down cleanly");
}
