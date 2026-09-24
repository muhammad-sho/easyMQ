import type { Redis } from "ioredis";
import { waitForRedisReady } from "../infrastructure/redis/connection-manager.js";
import { ApiError } from "../api/errors.js";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";

export interface CancellationMarker {
  queue: string;
  jobId: string;
  /** Attempt index the cancellation targets (job identity/version). */
  attemptsMade: number;
  requestedAtMs: number;
}

export interface CancellationOptions {
  keyPrefix: string;
  /** Seconds a cancellation marker lives (covers slow workers). */
  ttlSeconds: number;
}

/**
 * Distributed active-job cancellation.
 *
 * BullMQ's `Worker.cancelJob()` is instance-local, so easyMQ coordinates:
 *
 *   API -> verify job is active -> write TTL marker -> publish signal ->
 *   every worker receives it -> local `Worker.cancelJob()` ->
 *   AbortSignal reaches the executor -> attempt fails with the stable
 *   `easymq:cancelled` reason (UnrecoverableError => no retry).
 *
 * The marker carries `attemptsMade` so a reused job id (or a later retry
 * attempt) is never affected by a stale cancellation.
 *
 * Cooperative only: if the executor ignores the AbortSignal, the attempt
 * runs to its own outcome (documented race, see README).
 */
export class CancellationCoordinator {
  private readonly channel: string;
  private readonly keyPrefix: string;

  constructor(
    private readonly redis: Redis,
    private readonly createSubscriber: () => Redis,
    private readonly queues: QueueFactory,
    private readonly options: CancellationOptions,
    private readonly logger?: Logger,
  ) {
    this.keyPrefix = `${options.keyPrefix}:cancel`;
    this.channel = `${options.keyPrefix}:cancel-signal`;
  }

  private markerKey(queue: string, jobId: string): string {
    return `${this.keyPrefix}:${queue}:${jobId}`;
  }

  /**
   * Request cancellation of an active job. Idempotent — requesting twice
   * for the same attempt is a no-op success.
   */
  async requestCancellation(queue: string, jobId: string): Promise<CancellationMarker> {
    const bullQueue = this.queues.getQueue(queue);
    const job = await bullQueue.getJob(jobId);
    if (!job) throw ApiError.notFound("job", jobId, queue);
    const state = await job.getState();
    if (state !== "active") {
      throw new ApiError(
        "JOB_NOT_ACTIVE",
        `Only active jobs can be cancelled (job '${jobId}' is ${state}). ` +
          `Remove waiting or delayed jobs instead.`,
        { resource: { type: "job", id: jobId, queue } },
      );
    }
    const marker: CancellationMarker = {
      queue,
      jobId,
      attemptsMade: job.attemptsMade,
      requestedAtMs: Date.now(),
    };
    await this.redis.set(
      this.markerKey(queue, jobId),
      JSON.stringify(marker),
      "EX",
      this.options.ttlSeconds,
      "NX",
    );
    await this.redis.publish(this.channel, JSON.stringify({ queue, jobId }));
    this.logger?.info({ event: "cancellation-requested", queue, jobId }, "Cancellation requested");
    return marker;
  }

  /** True when the given attempt has been cancelled. Consumes nothing. */
  async isCancelled(queue: string, jobId: string, attemptsMade: number): Promise<boolean> {
    const raw = await this.redis.get(this.markerKey(queue, jobId));
    if (!raw) return false;
    try {
      const marker = JSON.parse(raw) as Partial<CancellationMarker>;
      return marker.jobId === jobId && marker.attemptsMade === attemptsMade;
    } catch {
      return false;
    }
  }

  /** True when any cancellation marker exists for the job (any attempt). */
  async hasMarker(queue: string, jobId: string): Promise<boolean> {
    const exists = await this.redis.exists(this.markerKey(queue, jobId));
    return exists === 1;
  }
  /** Remove the marker (best-effort hygiene after a terminal outcome). */
  async clearMarker(queue: string, jobId: string): Promise<void> {
    try {
      await this.redis.del(this.markerKey(queue, jobId));
    } catch (err) {
      this.logger?.warn({ err, queue, jobId }, "Failed to clear cancellation marker");
    }
  }

  /**
   * Worker-side: subscribe to cancellation signals. The handler receives
   * (queue, jobId) for every signal and should call the local Worker's
   * `cancelJob`. Reconciles on reconnect via marker TTL expiry semantics
   * (markers are short-lived; the pre-attempt check is authoritative).
   */
  async subscribeForWorkers(
    onSignal: (queue: string, jobId: string) => void,
  ): Promise<() => Promise<void>> {
    const subscriber = this.createSubscriber();
    await waitForRedisReady(subscriber, 15_000);
    const handler = (_channel: string, message: string): void => {
      try {
        const parsed = JSON.parse(message) as { queue?: unknown; jobId?: unknown };
        if (typeof parsed.queue === "string" && typeof parsed.jobId === "string") {
          onSignal(parsed.queue, parsed.jobId);
        }
      } catch (err) {
        this.logger?.warn({ err }, "Ignoring malformed cancellation signal");
      }
    };
    subscriber.on("message", handler);
    await subscriber.subscribe(this.channel);
    return async () => {
      subscriber.off("message", handler);
      try {
        await subscriber.unsubscribe(this.channel);
      } catch {
        // ignore — shutting down
      }
    };
  }
}
