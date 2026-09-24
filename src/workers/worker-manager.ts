import { Worker } from "bullmq";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { StoredJobData } from "../jobs/job-types.js";
import type { ExecutionResult } from "../executors/executor.js";
import type { RedisConnectionManager } from "../infrastructure/redis/connection-manager.js";
import type { QueueCatalog } from "../queues/queue-catalog.js";
import type {
  CancellationCoordinator,
  CancellationSignal,
  CancellationSubscription,
} from "./cancellation.js";
import type { JobProcessor } from "./job-processor.js";

export interface WorkerManagerOptions {
  concurrency: number;
  /** BullMQ key prefix (must match the API side). */
  prefix?: string;
  /** BullMQ lock TTL (ms). Defaults to BullMQ's own default when omitted. */
  lockDurationMs?: number;
  /** BullMQ stalled-job scan interval (ms). Defaults to BullMQ's own default. */
  stalledIntervalMs?: number;
}

interface ManagedQueue {
  worker: Worker<StoredJobData, ExecutionResult, string>;
}

export interface WorkerManagerStopOptions {
  /**
   * Grace period (ms) for active jobs before force-close.
   * <= 0 (default) waits indefinitely, preserving historical behavior.
   * The application always passes the configured shutdown deadline.
   */
  shutdownTimeoutMs?: number;
  /** Maximum extra wait (ms) for BullMQ's forced close to release resources. */
  forceGraceMs?: number;
}

export interface WorkerManagerDeps {
  connections: RedisConnectionManager;
  catalog: QueueCatalog;
  cancellation: CancellationCoordinator;
  processor: JobProcessor;
  options: WorkerManagerOptions;
  logger?: Logger;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Worker-role lifecycle: discovers registered queues (existing + new),
 * keeps one BullMQ Worker per queue, and routes distributed cancellation
 * prompts to the local Worker after validating them against live jobs.
 *
 * Multiple instances may manage the same queue — BullMQ distributes
 * work and locking across them.
 *
 * Shutdown: locally pause each worker first, so it cannot fetch more jobs
 * while active jobs drain. This deliberately defers `close()` until the
 * decision is made: BullMQ 6.3.8 memoizes close(), so calling close(false)
 * first would make a later close(true) unable to force-close. On deadline
 * expiry, in-flight attempts are aborted and the first close call is forced.
 * Locks left behind expire via BullMQ TTL and unfinished work is reclaimed
 * through BullMQ stalled-job recovery — no custom recovery.
 */
export class WorkerManager {
  private readonly managed = new Map<string, ManagedQueue>();
  private cancelSub: CancellationSubscription | undefined;
  private started = false;
  private stopping: Promise<void> | undefined;
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
    this.stopping = undefined;
    // 1. Subscribe to cancellation prompts BEFORE creating workers, then
    //    reconcile persisted markers against active jobs (covers markers
    //    written before subscribing and any missed notification).
    this.cancelSub = await this.deps.cancellation.subscribeForWorkers({
      // Hook failures are logged by the coordinator; the boolean result
      // is only useful to direct callers/tests.
      onSignal: (signal) => {
        void this.cancelSignal(signal);
      },
      onReconnect: () => this.reconcileCancellations(),
    });
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
        ...(options.lockDurationMs !== undefined ? { lockDuration: options.lockDurationMs } : {}),
        ...(options.stalledIntervalMs !== undefined
          ? { stalledInterval: options.stalledIntervalMs }
          : {}),
      },
    );

    worker.on("active", (job) => {
      logger?.info({ event: "job-active", queue: queueName, jobId: job.id }, "Job active");
    });
    worker.on("completed", (job) => {
      logger?.info({ event: "job-completed", queue: queueName, jobId: job.id }, "Job completed");
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

    this.managed.set(queueName, { worker });
    logger?.info({ event: "worker-created", queue: queueName }, "Worker created");
    return worker;
  }

  /**
   * Handle a cancellation prompt: validate generation + attempt against
   * the live job, then cancel locally. Stale prompts (completed jobs,
   * retries, recreated IDs) never match and are ignored.
   */
  async cancelSignal(signal: CancellationSignal): Promise<boolean> {
    return this.deps.cancellation.cancelMatching(signal, (queue, jobId) =>
      this.cancelLocal(queue, jobId),
    );
  }

  /** Reconcile persisted markers against active jobs (reconnect recovery). */
  async reconcileCancellations(): Promise<void> {
    try {
      await this.deps.cancellation.reconcileQueues(this.queueNames(), (queue, jobId) =>
        this.cancelLocal(queue, jobId),
      );
    } catch (err) {
      this.deps.logger?.warn(
        { err, event: "cancellation-reconcile-failed" },
        "Cancellation reconciliation failed",
      );
    }
  }

  /** Instance-local cancellation for a validated distributed signal. */
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

  /**
   * Stop workers and release subscriptions. Idempotent: concurrent calls
   * share one shutdown. Resolves in bounded time when a deadline is set.
   */
  async stop(options: WorkerManagerStopOptions = {}): Promise<void> {
    if (!this.stopping) {
      this.stopping = this.doStop(options);
    }
    return this.stopping;
  }

  private async doStop(options: WorkerManagerStopOptions): Promise<void> {
    this.started = false;
    this.deps.catalog.offQueueRegistered(this.onRegistered);

    // 1. Pause locally. pause(false) prevents future fetches while resolving
    // only once active jobs drain; importantly, it does not put Worker.close
    // into its irreversible memoized closing state.
    const managed = [...this.managed.entries()];
    this.managed.clear();
    const drained = Promise.all(
      managed.map(async ([name, { worker }]) => {
        try {
          await worker.pause(false);
        } catch (err) {
          this.deps.logger?.warn({ err, queue: name }, "Error pausing worker for shutdown");
        }
      }),
    );

    const deadlineMs = options.shutdownTimeoutMs ?? 0;
    let drainedBeforeDeadline = true;
    if (deadlineMs > 0) {
      drainedBeforeDeadline = await Promise.race([
        drained.then(() => true),
        sleep(deadlineMs).then(() => false),
      ]);
    } else {
      await drained;
    }

    if (!drainedBeforeDeadline) {
      this.deps.logger?.warn(
        { event: "shutdown-force", deadlineMs },
        "Shutdown deadline exceeded — aborting in-flight attempts and force-closing workers",
      );
      // Cooperative executors settle from the signal; non-cooperative ones
      // are detached by close(true), leaving BullMQ to recover their jobs.
      for (const [name, { worker }] of managed) {
        try {
          worker.cancelAllJobs("shutdown");
        } catch (err) {
          this.deps.logger?.warn({ err, queue: name }, "Error aborting worker jobs");
        }
      }
      const forceGraceMs = options.forceGraceMs ?? 5000;
      const forceClose = Promise.all(
        managed.map(async ([name, { worker }]) => {
          try {
            await worker.close(true);
          } catch (err) {
            this.deps.logger?.warn({ err, queue: name }, "Error force-closing worker");
          }
        }),
      );
      await Promise.race([forceClose, sleep(forceGraceMs)]);
    } else {
      await Promise.all(
        managed.map(async ([name, { worker }]) => {
          try {
            await worker.close();
          } catch (err) {
            this.deps.logger?.warn({ err, queue: name }, "Error closing worker");
          }
        }),
      );
    }

    // 2. Close cancellation subscription.
    const cancelSub = this.cancelSub;
    this.cancelSub = undefined;
    if (cancelSub) {
      try {
        await cancelSub.unsubscribe();
      } catch {
        // ignore — shutting down
      }
    }
    // 3. Stop catalog subscription.
    try {
      await this.deps.catalog.stop();
    } catch {
      // ignore — shutting down
    }
  }
}
