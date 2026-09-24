import { QueueEvents, Worker } from "bullmq";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { StoredJobData } from "../jobs/job-types.js";
import type { ExecutionResult } from "../executors/executor.js";
import type { RedisConnectionManager } from "../infrastructure/redis/connection-manager.js";
import type { QueueCatalog } from "../queues/queue-catalog.js";
import type { CancellationCoordinator } from "./cancellation.js";
import type { JobProcessor } from "./job-processor.js";

export interface WorkerManagerOptions {
  concurrency: number;
  /** BullMQ key prefix (must match the API side). */
  prefix?: string;
}

interface ManagedQueue {
  worker: Worker<StoredJobData, ExecutionResult, string>;
  events: QueueEvents;
}

export interface WorkerManagerDeps {
  connections: RedisConnectionManager;
  catalog: QueueCatalog;
  cancellation: CancellationCoordinator;
  processor: JobProcessor;
  options: WorkerManagerOptions;
  logger?: Logger;
}

/**
 * Worker-role lifecycle: discovers registered queues (existing + new),
 * keeps one BullMQ Worker per queue, wires QueueEvents, and routes
 * distributed cancellation signals to the local Worker.
 *
 * Multiple instances may manage the same queue — BullMQ distributes
 * work and locking across them.
 */
export class WorkerManager {
  private readonly managed = new Map<string, ManagedQueue>();
  private unsubscribeCancellation: (() => Promise<void>) | undefined;
  private started = false;
  private readonly onRegistered = (queueName: string): void => {
    try {
      this.ensureWorker(queueName);
    } catch (err) {
      this.deps.logger?.error({ err, queue: queueName }, "Failed to create worker");
    }
  };

  constructor(private readonly deps: WorkerManagerDeps) {}

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    // 1. Subscribe to cancellation signals first.
    this.unsubscribeCancellation = await this.deps.cancellation.subscribeForWorkers(
      (queue, jobId) => this.cancelLocal(queue, jobId),
    );
    // 2. Load existing registry + listen for new registrations.
    this.deps.catalog.onQueueRegistered(this.onRegistered);
    await this.deps.catalog.start();
    this.deps.logger?.info(
      { event: "worker-manager-started", queues: this.managed.size },
      "Worker manager started",
    );
  }

  /** Idempotent: returns the existing Worker when already managed. */
  ensureWorker(queueName: string): Worker<StoredJobData, ExecutionResult, string> {
    const existing = this.managed.get(queueName);
    if (existing) return existing.worker;

    const { connections, processor, options, logger } = this.deps;
    const worker = new Worker<StoredJobData, ExecutionResult, string>(
      queueName,
      processor.handler(queueName),
      {
        connection: connections.createDedicated(`worker:${queueName}`),
        concurrency: options.concurrency,
        ...(options.prefix !== undefined ? { prefix: options.prefix } : {}),
      },
    );
    const events = new QueueEvents(queueName, {
      connection: connections.createDedicated(`events:${queueName}`),
      ...(options.prefix !== undefined ? { prefix: options.prefix } : {}),
    });

    worker.on("active", (job) => {
      logger?.info({ event: "job-active", queue: queueName, jobId: job.id }, "Job active");
    });
    worker.on("completed", (job) => {
      logger?.info(
        { event: "job-completed", queue: queueName, jobId: job.id },
        "Job completed",
      );
    });
    worker.on("failed", (job, err) => {
      logger?.warn(
        { event: "job-failed", queue: queueName, jobId: job?.id, failedReason: err.message },
        "Job failed",
      );
    });
    worker.on("stalled", (jobId) => {
      logger?.warn({ event: "job-stalled", queue: queueName, jobId }, "Job stalled");
    });
    worker.on("error", (err) => {
      logger?.error({ err, event: "worker-error", queue: queueName }, "Worker error");
    });
    events.on("error", (err) => {
      logger?.error(
        { err, event: "queue-events-error", queue: queueName },
        "QueueEvents error",
      );
    });

    this.managed.set(queueName, { worker, events });
    logger?.info({ event: "worker-created", queue: queueName }, "Worker created");
    return worker;
  }

  /** Instance-local cancellation for a distributed signal. */
  cancelLocal(queueName: string, jobId: string): boolean {
    const managed = this.managed.get(queueName);
    if (!managed) return false;
    const cancelled = managed.worker.cancelJob(jobId);
    if (cancelled) {
      this.deps.logger?.info(
        { event: "job-cancel-local", queue: queueName, jobId },
        "Local job cancellation triggered",
      );
    }
    return cancelled;
  }

  queueNames(): string[] {
    return [...this.managed.keys()];
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    this.deps.catalog.offQueueRegistered(this.onRegistered);

    // 1. Stop workers from fetching new jobs; let active jobs finish
    //    within the caller's shutdown deadline (enforced upstream).
    const managed = [...this.managed.entries()];
    this.managed.clear();
    await Promise.all(
      managed.map(async ([name, { worker }]) => {
        try {
          await worker.close();
        } catch (err) {
          this.deps.logger?.warn({ err, queue: name }, "Error closing worker");
        }
      }),
    );
    // 2. Close QueueEvents.
    await Promise.all(
      managed.map(async ([name, { events }]) => {
        try {
          await events.close();
        } catch (err) {
          this.deps.logger?.warn({ err, queue: name }, "Error closing queue events");
        }
      }),
    );
    // 3. Close cancellation subscription.
    if (this.unsubscribeCancellation) {
      const unsub = this.unsubscribeCancellation;
      this.unsubscribeCancellation = undefined;
      try {
        await unsub();
      } catch {
        // ignore — shutting down
      }
    }
    // 4. Stop catalog subscription.
    try {
      await this.deps.catalog.stop();
    } catch {
      // ignore — shutting down
    }
  }
}
