import type { AddressInfo } from "node:net";
import { loadConfig, type AppConfig } from "../../src/config/env.js";
import { QueueCatalog } from "../../src/queues/queue-catalog.js";
import { QueueFactory } from "../../src/infrastructure/bullmq/queue-factory.js";
import { RedisConnectionManager } from "../../src/infrastructure/redis/connection-manager.js";
import { JobService } from "../../src/jobs/job-service.js";
import { ScheduleService } from "../../src/jobs/schedule-service.js";
import { QueueService } from "../../src/queues/queue-service.js";
import { CancellationCoordinator } from "../../src/workers/cancellation.js";
import { JobProcessor } from "../../src/workers/job-processor.js";
import { WorkerManager } from "../../src/workers/worker-manager.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import {
  ExecutionAbortedError,
  type ExecutionResult,
  type Executor,
  type JobExecutionContext,
} from "../../src/executors/executor.js";
import type { EasyMQJob } from "../../src/jobs/job-types.js";

export const REDIS_URL = process.env["REDIS_URL"] ?? "redis://127.0.0.1:6379";

export function uniquePrefix(tag: string): string {
  const rand = Math.floor(Math.random() * 1_000_000).toString(36);
  return `it-${tag}-${process.pid.toString(36)}-${rand}`;
}

export function testConfig(prefix: string, env: Record<string, string> = {}): AppConfig {
  return loadConfig({
    AUTH_DISABLED: "true",
    REDIS_URL,
    REDIS_KEY_PREFIX: prefix,
    WORKER_CONCURRENCY: "5",
    ...env,
  });
}

export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<void> {
  const { timeoutMs = 20_000, intervalMs = 100, label = "condition" } = options;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await condition()) return;
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${label} after ${String(timeoutMs)}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

export async function waitForJob(
  jobService: JobService,
  queue: string,
  jobId: string,
  state: EasyMQJob["state"],
  timeoutMs = 20_000,
): Promise<EasyMQJob> {
  let latest: EasyMQJob | undefined;
  await waitFor(
    async () => {
      latest = await jobService.getJob(queue, jobId);
      return latest.state === state;
    },
    { timeoutMs, label: `job ${jobId} to reach ${state}` },
  );
  // The job hash and its state live in different Redis keys, so a fast
  // worker can slip an attempt between the two reads (torn read). Settle
  // with a fresh fetch before asserting on hash fields.
  await new Promise((resolve) => setTimeout(resolve, 250));
  return jobService.getJob(queue, jobId);
}

export type FakeBehavior =
  | { kind: "success"; statusCode?: number }
  | { kind: "fail"; message?: string }
  | { kind: "failOnce" }
  | { kind: "block" };

/** Fake executor for integration tests (no external HTTP needed). */
export class FakeExecutor implements Executor {
  readonly type = "http" as const;
  readonly calls: Array<{ jobId: string; attemptsMade: number }> = [];

  constructor(private behavior: FakeBehavior = { kind: "success" }) {}

  setBehavior(behavior: FakeBehavior): void {
    this.behavior = behavior;
  }

  async execute(ctx: JobExecutionContext): Promise<ExecutionResult> {
    this.calls.push({ jobId: ctx.jobId, attemptsMade: ctx.attemptsMade });
    const behavior = this.behavior;
    switch (behavior.kind) {
      case "success":
        return {
          statusCode: behavior.statusCode ?? 200,
          headers: {},
          body: JSON.stringify({ echo: ctx.payload }),
          bodyTruncated: false,
          durationMs: 0,
        };
      case "fail":
        throw new Error(behavior.message ?? "fake failure");
      case "failOnce":
        if (ctx.attemptsMade === 0) throw new Error("first attempt fails");
        return {
          statusCode: 200,
          headers: {},
          body: "recovered",
          bodyTruncated: false,
          durationMs: 0,
        };
      case "block": {
        if (ctx.signal.aborted) throw new ExecutionAbortedError();
        await new Promise<void>((_resolve, reject) => {
          ctx.signal.addEventListener("abort", () => reject(new ExecutionAbortedError()), {
            once: true,
          });
        });
        throw new ExecutionAbortedError();
      }
    }
  }
}

export interface TestSystem {
  config: AppConfig;
  connections: RedisConnectionManager;
  catalog: QueueCatalog;
  queues: QueueFactory;
  cancellation: CancellationCoordinator;
  processor: JobProcessor;
  manager: WorkerManager;
  queueService: QueueService;
  jobService: JobService;
  scheduleService: ScheduleService;
  fake: FakeExecutor;
}

/** Full worker-side stack with a fake executor. */
export function buildTestSystem(prefix: string, env: Record<string, string> = {}): TestSystem {
  const config = testConfig(prefix, env);
  const logger = createLogger({ level: "silent", role: "both" });
  const connections = new RedisConnectionManager(REDIS_URL, logger);
  const shared = connections.getShared();
  const catalog = new QueueCatalog(
    shared,
    () => connections.createDedicated("catalog-sub"),
    { keyPrefix: prefix },
    logger,
  );
  const queues = new QueueFactory(connections, { prefix }, logger);
  const cancellation = new CancellationCoordinator(
    shared,
    () => connections.createDedicated("cancel-sub"),
    queues,
    { keyPrefix: prefix, ttlSeconds: 60 },
    logger,
  );
  const fake = new FakeExecutor();
  const processor = new JobProcessor({ executors: [fake] }, cancellation, logger);
  const manager = new WorkerManager({
    connections,
    catalog,
    cancellation,
    processor,
    options: { concurrency: 5, prefix },
    logger,
  });
  const queueService = new QueueService(queues, catalog);
  const jobService = new JobService(queues, catalog, config, cancellation);
  const scheduleService = new ScheduleService(queues, catalog, config);
  return {
    config,
    connections,
    catalog,
    queues,
    cancellation,
    processor,
    manager,
    queueService,
    jobService,
    scheduleService,
    fake,
  };
}

export async function closeTestSystem(system: TestSystem): Promise<void> {
  try {
    await system.manager.stop();
  } catch {
    // ignore
  }
  try {
    await system.catalog.stop();
  } catch {
    // ignore
  }
  try {
    await system.queues.closeAll();
  } catch {
    // ignore
  }
  await system.connections.closeAll();
}

export function ephemeralPort(server: { address(): unknown }): number {
  return (server.address() as AddressInfo).port;
}

export type { AppConfig };
