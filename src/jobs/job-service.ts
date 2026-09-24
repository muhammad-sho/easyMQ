import { ErrorCode, UnrecoverableError, type Job, type JobsOptions, type JobType } from "bullmq";
import { ApiError, classifyBackendError } from "../api/errors.js";
import type { AppConfig } from "../config/schema.js";
import type { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";
import {
  CANCELLATION_FAILED_REASON,
  type CreateJobOptions,
  type EasyMQJob,
  type EasyMQJobState,
  type Execution,
  type HttpExecution,
  type Json,
  type ListJobsOptions,
  type PublicExecution,
  type RetentionOptions,
  type StoredJobData,
} from "./job-types.js";
import { assertValidQueueName, type QueueCatalog } from "../queues/queue-catalog.js";

const MAX_PRIORITY = 2_097_151;

const LISTABLE_STATES: JobType[] = [
  "active",
  "completed",
  "delayed",
  "failed",
  "prioritized",
  "waiting",
  "waiting-children",
];

function toJobType(state: EasyMQJobState): JobType | undefined {
  if (state === "unknown") return undefined;
  return state;
}

/**
 * BullMQ reports "job is not in the expected state" (e.g. promoting or
 * re-delaying a non-delayed job) as a plain Error carrying the numeric
 * finishedErrors code JobNotInState (-3). That is a deterministic caller
 * conflict, not a backend failure. (BullMQ's DelayedError class signals
 * something else — active→delayed moves — and is not relevant here.)
 */
function isJobNotInStateError(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;
  return (err as { code?: unknown }).code === ErrorCode.JobNotInState;
}

/** Extract easyMQ payload/execution from BullMQ job data (never throws). */
export function parseStoredData(data: unknown): {
  payload: Json | null;
  execution: Execution | null;
} {
  if (typeof data !== "object" || data === null) {
    return { payload: null, execution: null };
  }
  const record = data as {
    version?: unknown;
    payload?: Json | undefined;
    execution?: unknown;
  };
  if (record.version !== 1) return { payload: null, execution: null };
  const execution: unknown = record.execution;
  if (!isHttpExecution(execution)) {
    return { payload: null, execution: null };
  }
  return {
    payload: record.payload ?? null,
    execution,
  };
}

function isHttpExecution(value: unknown): value is HttpExecution {
  if (typeof value !== "object" || value === null) return false;
  if (!("type" in value) || !("url" in value)) return false;
  return value.type === "http" && typeof value.url === "string";
}

/**
 * Map stored execution config to its API-safe view. Header VALUES never
 * leave the server (the worker reads them from stored job data); only
 * header names are exposed. Stored data is never mutated.
 */
export function toPublicExecution(execution: Execution): PublicExecution {
  const headers = execution.headers ?? {};
  return {
    type: "http",
    url: execution.url,
    ...(execution.method !== undefined ? { method: execution.method } : {}),
    ...(Object.keys(headers).length > 0 ? { headerNames: Object.keys(headers) } : {}),
    ...(execution.body !== undefined ? { body: execution.body } : {}),
    ...(execution.timeoutMs !== undefined ? { timeoutMs: execution.timeoutMs } : {}),
  };
}

export async function toEasyMQJob(job: Job): Promise<EasyMQJob> {
  let state: EasyMQJobState;
  try {
    state = await job.getState();
  } catch (err) {
    throw classifyBackendError(err, "job inspection");
  }
  const { payload, execution } = parseStoredData(job.data);
  if (job.id === undefined) {
    throw ApiError.internal("Job is missing its id.");
  }
  return {
    id: job.id,
    queue: job.queueName,
    name: job.name,
    state,
    payload,
    execution: execution ? toPublicExecution(execution) : null,
    attemptsMade: job.attemptsMade,
    priority: job.opts.priority ?? 0,
    delayMs: job.opts.delay ?? 0,
    timestampMs: job.timestamp,
    processedOnMs: job.processedOn ?? null,
    finishedOnMs: job.finishedOn ?? null,
    failedReason: job.failedReason ?? null,
    returnValue: (job.returnvalue ?? null) as Json | null,
  };
}

function mapRetention(
  retention: RetentionOptions | undefined,
  defaultCount: number,
): number | { count: number } | { age: number; count?: number } {
  if (!retention || (retention.count === undefined && retention.ageSeconds === undefined)) {
    return { count: defaultCount };
  }
  const hasCount = retention.count !== undefined;
  const hasAge = retention.ageSeconds !== undefined;
  if (hasCount && hasAge) {
    return {
      count: retention.count as number,
      age: retention.ageSeconds as number,
    };
  }
  if (hasCount) return { count: retention.count as number };
  return { age: retention.ageSeconds as number };
}

/**
 * Translate easyMQ job options to BullMQ JobsOptions.
 * `debounce` is expressed through `deduplication` (BullMQ 6.x removed
 * the old `debounce` option): same id + delay + replace semantics.
 */
export function toBullMQJobOptions(
  opts: CreateJobOptions,
  defaults: {
    attempts: number;
    backoffType: "fixed" | "exponential";
    backoffDelayMs: number;
    removeOnCompleteCount: number;
    removeOnFailCount: number;
  },
): JobsOptions {
  const jobOptions: JobsOptions = {};

  if (opts.jobId !== undefined) jobOptions.jobId = opts.jobId;
  if (opts.delayMs !== undefined) jobOptions.delay = opts.delayMs;
  if (opts.lifo !== undefined) jobOptions.lifo = opts.lifo;

  jobOptions.attempts = opts.attempts ?? defaults.attempts;
  if (opts.backoff) {
    jobOptions.backoff = { type: opts.backoff.type, delay: opts.backoff.delayMs };
  } else {
    jobOptions.backoff = {
      type: defaults.backoffType,
      delay: defaults.backoffDelayMs,
    };
  }

  if (opts.priority !== undefined) {
    if (!Number.isInteger(opts.priority) || opts.priority < 0 || opts.priority > MAX_PRIORITY) {
      throw ApiError.validation(`Priority must be an integer between 0 and ${MAX_PRIORITY}.`, {
        priority: opts.priority,
      });
    }
    jobOptions.priority = opts.priority;
  }

  if (opts.deduplication && opts.debounce) {
    throw ApiError.validation("Only one of deduplication or debounce may be set.");
  }
  if (opts.deduplication) {
    jobOptions.deduplication = {
      id: opts.deduplication.id,
      ...(opts.deduplication.ttlMs !== undefined ? { ttl: opts.deduplication.ttlMs } : {}),
      ...(opts.deduplication.replace !== undefined ? { replace: opts.deduplication.replace } : {}),
    };
  } else if (opts.debounce) {
    jobOptions.delay = opts.debounce.delayMs;
    jobOptions.deduplication = {
      id: opts.debounce.id,
      ttl: opts.debounce.delayMs,
      replace: opts.debounce.replace ?? true,
    };
  }

  jobOptions.removeOnComplete = mapRetention(opts.removeOnComplete, defaults.removeOnCompleteCount);
  jobOptions.removeOnFail = mapRetention(opts.removeOnFail, defaults.removeOnFailCount);

  return jobOptions;
}

export interface JobPage {
  jobs: EasyMQJob[];
  offset: number;
  limit: number;
  nextOffset: number | null;
}

export interface CancelJobDeps {
  requestCancellation(queue: string, jobId: string): Promise<unknown>;
}

/**
 * Application service for jobs. Owns the easyMQ <-> BullMQ translation
 * so BullMQ objects never leak into the public API.
 */
export class JobService {
  constructor(
    private readonly queues: QueueFactory,
    private readonly catalog: QueueCatalog,
    private readonly config: AppConfig,
    private readonly canceller?: CancelJobDeps,
  ) {}

  private defaults() {
    return {
      attempts: this.config.defaultAttempts,
      backoffType: this.config.defaultBackoffType,
      backoffDelayMs: this.config.defaultBackoffDelayMs,
      removeOnCompleteCount: this.config.defaultRemoveOnCompleteCount,
      removeOnFailCount: this.config.defaultRemoveOnFailCount,
    };
  }

  async createJob(opts: CreateJobOptions): Promise<EasyMQJob> {
    assertValidQueueName(opts.queue);
    // Register BEFORE enqueueing so worker instances can always discover
    // the new work. A failed enqueue may leave an empty registration behind;
    // that is harmless and never rolled back (another instance may already
    // be using the queue). Registry outages fail the request via
    // classification: the job is never claimed as accepted
    // when registration did not complete.
    try {
      await this.catalog.register(opts.queue);
    } catch (err) {
      throw classifyBackendError(err, "queue registration");
    }
    const data: StoredJobData = {
      version: 1,
      payload: opts.payload ?? null,
      execution: opts.execution,
    };
    const queue = this.queues.getQueue(opts.queue);
    let job: Job;
    try {
      job = await queue.add(
        opts.name ?? "default",
        data,
        toBullMQJobOptions(opts, this.defaults()),
      );
    } catch (err) {
      throw this.mapAddError(err, opts.queue);
    }
    return toEasyMQJob(job);
  }

  async getJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    return toEasyMQJob(job);
  }

  async listJobs(opts: ListJobsOptions): Promise<JobPage> {
    assertValidQueueName(opts.queue);
    await this.ensureRegistered(opts.queue);
    const limit = Math.min(
      Math.max(opts.limit ?? this.config.pageDefaultLimit, 1),
      this.config.pageMaxLimit,
    );
    const offset = Math.max(opts.offset ?? 0, 0);
    const asc = opts.asc ?? false;

    let types: JobType[] | undefined;
    if (opts.states && opts.states.length > 0) {
      const mapped = opts.states.map(toJobType).filter((t): t is JobType => t !== undefined);
      if (mapped.length === 0) {
        return { jobs: [], offset, limit, nextOffset: null };
      }
      types = [...new Set(mapped)];
    }

    const queue = this.queues.getQueue(opts.queue);
    // BullMQ merges per-type ranges when several types are requested, so
    // paginate per type and slice globally for an exact limit/offset page.
    // Ordering: jobs are concatenated in the requested state order, each in
    // BullMQ index order (asc parameter). Documented in the README.
    // Fetch one extra item so `nextOffset` is exact, including when the
    // result count is an exact multiple of the page size.
    const window = offset + limit + 1;
    const seen = new Set<string>();
    const merged: Job[] = [];
    try {
      for (const type of types ?? LISTABLE_STATES) {
        const batch = await queue.getJobs([type], 0, window - 1, asc);
        for (const job of batch) {
          if (job.id !== undefined && !seen.has(job.id)) {
            seen.add(job.id);
            merged.push(job);
          }
        }
        if (merged.length >= window) break;
      }
    } catch (err) {
      throw classifyBackendError(err, "job listing");
    }
    const page = merged.slice(offset, offset + limit);
    const mapped = await Promise.all(page.map((job) => toEasyMQJob(job)));
    return {
      jobs: mapped,
      offset,
      limit,
      nextOffset: mapped.length === limit && merged.length > offset + limit ? offset + limit : null,
    };
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const queue = this.queues.getQueue(queueName);
    let job: Job | undefined;
    try {
      job = await queue.getJob(jobId);
    } catch (err) {
      throw classifyBackendError(err, "job inspection");
    }
    if (!job) throw ApiError.notFound("job", jobId, queueName);
    let removed: number;
    try {
      removed = await queue.remove(jobId);
    } catch (err) {
      throw classifyBackendError(err, "job removal");
    }
    if (removed === 0) {
      throw new ApiError("CONFLICT", `Job '${jobId}' is currently locked and cannot be removed.`, {
        resource: { type: "job", id: jobId, queue: queueName },
      });
    }
  }

  /** Manually retry a finished (failed or completed) job. */
  async retryJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    let state: string;
    try {
      state = await job.getState();
    } catch (err) {
      throw classifyBackendError(err, "job inspection");
    }
    if (state !== "failed" && state !== "completed") {
      throw new ApiError(
        "CONFLICT",
        `Only failed or completed jobs can be retried (job '${jobId}' is ${state}).`,
        { resource: { type: "job", id: jobId, queue: queueName } },
      );
    }
    try {
      await job.retry(state);
    } catch (err) {
      throw classifyBackendError(err, "job retry");
    }
    return toEasyMQJob(job);
  }

  /** Promote a delayed job so it runs as soon as possible. */
  async promoteJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    try {
      await job.promote();
    } catch (err) {
      if (isJobNotInStateError(err)) {
        throw new ApiError(
          "CONFLICT",
          `Job '${jobId}' cannot be promoted (only delayed jobs can be promoted).`,
          { resource: { type: "job", id: jobId, queue: queueName }, cause: err },
        );
      }
      throw classifyBackendError(err, "job promotion");
    }
    return toEasyMQJob(job);
  }

  /** Change the delay of a delayed job. */
  async changeJobDelay(queueName: string, jobId: string, delayMs: number): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    try {
      await job.changeDelay(delayMs);
    } catch (err) {
      if (isJobNotInStateError(err)) {
        throw new ApiError(
          "CONFLICT",
          `Delay of job '${jobId}' cannot be changed (only delayed jobs support this).`,
          { resource: { type: "job", id: jobId, queue: queueName }, cause: err },
        );
      }
      throw classifyBackendError(err, "job delay change");
    }
    return toEasyMQJob(job);
  }

  async cancelJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    if (!this.canceller) {
      throw ApiError.internal("Cancellation support is not configured.");
    }
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    try {
      await this.canceller.requestCancellation(queueName, jobId);
    } catch (err) {
      throw classifyBackendError(err, "job cancellation");
    }
    return this.getJob(queueName, jobId);
  }

  private async requireJob(queueName: string, jobId: string): Promise<Job> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    let job: Job | undefined;
    try {
      job = await this.queues.getQueue(queueName).getJob(jobId);
    } catch (err) {
      throw classifyBackendError(err, "job inspection");
    }
    if (!job) throw ApiError.notFound("job", jobId, queueName);
    return job;
  }

  private async ensureRegistered(name: string): Promise<void> {
    const registered = await this.catalog.list();
    if (!registered.includes(name)) {
      throw ApiError.notFound("queue", name);
    }
  }

  private mapAddError(err: unknown, queue: string): ApiError {
    if (err instanceof ApiError) return err;
    const message = err instanceof Error ? err.message : String(err);
    if (/duplicat/i.test(message)) {
      return new ApiError("CONFLICT", "A job with the same deduplication id already exists.", {
        resource: { type: "queue", id: queue },
        cause: err,
      });
    }
    return classifyBackendError(err, "job creation");
  }
}

/** Build the terminal failure for a cancelled attempt (never retried). */
export function cancelledError(): UnrecoverableError {
  return new UnrecoverableError(CANCELLATION_FAILED_REASON);
}
