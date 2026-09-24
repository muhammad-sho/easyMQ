import type { Redis } from "ioredis";
import { waitForRedisReady } from "../infrastructure/redis/connection-manager.js";
import { ApiError, classifyBackendError } from "../api/errors.js";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";

export interface CancellationMarker {
  queue: string;
  jobId: string;
  /**
   * Immutable BullMQ creation timestamp: identifies the job GENERATION.
   * A removed job ID reused by a new job always has a different timestamp,
   * so stale markers can never affect it.
   */
  jobTimestampMs: number;
  /** Attempt index the cancellation targets. */
  attemptsMade: number;
  requestedAtMs: number;
}

export interface CancellationSignal {
  queue: string;
  jobId: string;
  jobTimestampMs: number;
  attemptsMade: number;
}

export interface CancellationWorkerHooks {
  /**
   * Prompt signal for a cancellation. The manager validates the signal
   * against the live job (active state, same generation + attempt) and
   * then cancels locally. May be async.
   */
  onSignal: (signal: CancellationSignal) => void | Promise<void>;
  /**
   * Called on initial subscribe and every subscriber reconnect: reconcile
   * persisted markers against currently active jobs (pub/sub is only a
   * prompt, never the durable record). No polling involved.
   */
  onReconnect: () => void | Promise<void>;
}

export interface CancellationSubscription {
  unsubscribe: () => Promise<void>;
  /** The live subscriber client (diagnostics, tests). */
  subscriber: Redis;
}

export interface CancellationOptions {
  keyPrefix: string;
  /** Seconds a cancellation marker lives (covers slow workers). */
  ttlSeconds: number;
}

/**
 * Atomically (re)write the marker and publish the prompt signal.
 * The marker always reflects the LATEST request: repeated requests for
 * the same attempt rewrite identical identity values (safe), while a
 * request for a newer attempt supersedes the old one.
 */
const REQUEST_SCRIPT = `
local created = redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('PUBLISH', KEYS[2], ARGV[3])
return created
`;

/** Delete the marker only when generation + attempt still match. */
const CLEAR_IF_MATCH_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local marker = cjson.decode(raw)
if marker.jobTimestampMs == tonumber(ARGV[1]) and marker.attemptsMade == tonumber(ARGV[2]) then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

function isCancellationSignal(value: unknown): value is CancellationSignal {
  if (typeof value !== "object" || value === null) return false;
  if (!("queue" in value) || !("jobId" in value)) return false;
  if (!("jobTimestampMs" in value) || !("attemptsMade" in value)) return false;
  const signal = value as Record<string, unknown>;
  return (
    typeof signal["queue"] === "string" &&
    typeof signal["jobId"] === "string" &&
    typeof signal["jobTimestampMs"] === "number" &&
    typeof signal["attemptsMade"] === "number"
  );
}

/**
 * Distributed active-job cancellation.
 *
 * BullMQ's `Worker.cancelJob()` is instance-local, so easyMQ coordinates:
 *
 *   API -> verify job is active -> atomically write TTL marker + publish
 *   prompt -> every worker validates generation + attempt against the
 *   live job -> local `Worker.cancelJob()` -> AbortSignal reaches the
 *   executor -> attempt fails with the stable `easymq:cancelled` reason
 *   (UnrecoverableError => no retry).
 *
 * Identity = (queue, jobId, jobTimestampMs, attemptsMade). The immutable
 * creation timestamp distinguishes job generations sharing an ID; the
 * attempt index scopes the cancellation to one attempt.
 *
 * Cooperative semantics (documented races):
 * - An attempt cancelled before/during execution fails unrecoverably.
 * - If the attempt completes first, the BullMQ terminal result stands;
 *   a marker written for it cannot affect retries (new attempt index)
 *   or recreated jobs (new timestamp).
 * - If the executor ignores the abort and fails on its own, normal retry
 *   policy applies to the new attempt; cancel again to target it.
 */
export class CancellationCoordinator {
  private readonly channel: string;
  private readonly keyPrefix: string;
  private readonly subscribers = new Set<Redis>();

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
   * A live cancellation subscriber, if this coordinator has any.
   * With several worker managers sharing one coordinator there is one
   * subscriber per manager; any of them serves for diagnostics.
   */
  getSubscriber(): Redis | undefined {
    return this.subscribers.values().next().value;
  }

  /**
   * Request cancellation of an active job. Safe to repeat for the same
   * attempt; a request for a newer attempt supersedes the marker.
   * A job that completes first keeps its terminal result.
   */
  async requestCancellation(queue: string, jobId: string): Promise<CancellationMarker> {
    let job;
    try {
      const bullQueue = this.queues.getQueue(queue);
      job = await bullQueue.getJob(jobId);
    } catch (err) {
      throw classifyBackendError(err, "job inspection");
    }
    if (!job) throw ApiError.notFound("job", jobId, queue);
    let state: string;
    try {
      state = await job.getState();
    } catch (err) {
      throw classifyBackendError(err, "job inspection");
    }
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
      jobTimestampMs: job.timestamp,
      attemptsMade: job.attemptsMade,
      requestedAtMs: Date.now(),
    };
    const signal: CancellationSignal = {
      queue,
      jobId,
      jobTimestampMs: job.timestamp,
      attemptsMade: job.attemptsMade,
    };
    try {
      await this.redis.eval(
        REQUEST_SCRIPT,
        2,
        this.markerKey(queue, jobId),
        this.channel,
        JSON.stringify(marker),
        String(this.options.ttlSeconds),
        JSON.stringify(signal),
      );
    } catch (err) {
      throw classifyBackendError(err, "job cancellation");
    }
    this.logger?.info({ event: "cancellation-requested", queue, jobId }, "Cancellation requested");
    return marker;
  }

  /**
   * True when the marker identifies this exact job generation + attempt.
   * Consumes nothing.
   */
  async isCancelled(
    queue: string,
    jobId: string,
    jobTimestampMs: number,
    attemptsMade: number,
  ): Promise<boolean> {
    let raw: string | null;
    try {
      raw = await this.redis.get(this.markerKey(queue, jobId));
    } catch (err) {
      throw classifyBackendError(err, "cancellation check");
    }
    if (!raw) return false;
    try {
      const marker = JSON.parse(raw) as Partial<CancellationMarker>;
      return (
        marker.jobId === jobId &&
        marker.jobTimestampMs === jobTimestampMs &&
        marker.attemptsMade === attemptsMade
      );
    } catch {
      return false;
    }
  }

  /**
   * Delete the marker, but only when it still belongs to the same job
   * generation + attempt (atomic compare-and-delete). TTL remains as
   * final cleanup if the process crashes before clearing.
   */
  async clearMarkerIfMatch(
    queue: string,
    jobId: string,
    jobTimestampMs: number,
    attemptsMade: number,
  ): Promise<boolean> {
    try {
      const removed = (await this.redis.eval(
        CLEAR_IF_MATCH_SCRIPT,
        1,
        this.markerKey(queue, jobId),
        String(jobTimestampMs),
        String(attemptsMade),
      )) as number;
      return removed === 1;
    } catch (err) {
      this.logger?.warn({ err, queue, jobId }, "Failed to clear cancellation marker");
      return false;
    }
  }

  /**
   * Validate a prompt signal against the live job and cancel locally
   * only on exact generation + attempt match of an ACTIVE job.
   * Returns true when a local cancel was triggered.
   */
  async cancelMatching(
    signal: CancellationSignal,
    cancelLocal: (queue: string, jobId: string) => boolean,
  ): Promise<boolean> {
    let job;
    try {
      job = await this.queues.getQueue(signal.queue).getJob(signal.jobId);
    } catch (err) {
      this.logger?.warn({ err, queue: signal.queue }, "Cancellation signal lookup failed");
      return false;
    }
    if (!job) return false;
    let state: string;
    try {
      state = await job.getState();
    } catch (err) {
      this.logger?.warn({ err, queue: signal.queue }, "Cancellation signal state check failed");
      return false;
    }
    if (state !== "active") return false;
    if (job.timestamp !== signal.jobTimestampMs || job.attemptsMade !== signal.attemptsMade) {
      return false;
    }
    return cancelLocal(signal.queue, signal.jobId);
  }

  /**
   * One-time reconciliation for the given queues: find ACTIVE jobs with
   * a matching marker and cancel them locally. Called on initial
   * subscribe and every subscriber reconnect — never on a timer.
   */
  async reconcileQueues(
    queueNames: string[],
    cancelLocal: (queue: string, jobId: string) => boolean,
  ): Promise<void> {
    for (const queueName of queueNames) {
      let active;
      try {
        active = await this.queues.getQueue(queueName).getJobs(["active"], 0, -1);
      } catch (err) {
        this.logger?.warn({ err, queue: queueName }, "Cancellation reconcile listing failed");
        continue;
      }
      for (const job of active) {
        if (job.id === undefined) continue;
        try {
          if (await this.isCancelled(queueName, job.id, job.timestamp, job.attemptsMade)) {
            cancelLocal(queueName, job.id);
          }
        } catch (err) {
          this.logger?.warn(
            { err, queue: queueName, jobId: job.id },
            "Cancellation reconcile check failed",
          );
        }
      }
    }
  }

  /**
   * Worker-side: subscribe to cancellation prompts. Reconciles markers
   * against active jobs on subscribe and on every reconnect, so a lost
   * pub/sub notification is recovered without polling.
   */
  async subscribeForWorkers(hooks: CancellationWorkerHooks): Promise<CancellationSubscription> {
    const subscriber = this.createSubscriber();
    await waitForRedisReady(subscriber, 15_000);
    this.subscribers.add(subscriber);
    let closed = false;

    const runHook = (label: string, fn: () => void | Promise<void>): void => {
      void Promise.resolve()
        .then(fn)
        .catch((err: unknown) => {
          this.logger?.warn({ err, event: label }, "Cancellation hook failed");
        });
    };

    const onMessage = (_channel: string, message: string): void => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(message) as unknown;
      } catch (err) {
        this.logger?.warn({ err }, "Ignoring malformed cancellation signal");
        return;
      }
      if (!isCancellationSignal(parsed)) {
        this.logger?.warn("Ignoring malformed cancellation signal");
        return;
      }
      const signal: CancellationSignal = parsed;
      runHook("cancellation-signal", () => hooks.onSignal(signal));
    };
    const onReady = (): void => {
      if (closed) return;
      runHook("cancellation-reconcile", () => hooks.onReconnect());
    };
    subscriber.on("message", onMessage);
    subscriber.on("ready", onReady);
    await subscriber.subscribe(this.channel);
    // Initial reconciliation covers markers written before subscribing.
    onReady();

    return {
      unsubscribe: async () => {
        closed = true;
        this.subscribers.delete(subscriber);
        subscriber.off("message", onMessage);
        subscriber.off("ready", onReady);
        try {
          await subscriber.unsubscribe(this.channel);
        } catch {
          // ignore — shutting down
        }
      },
      subscriber,
    };
  }
}
