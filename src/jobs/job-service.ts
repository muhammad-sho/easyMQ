import { UnrecoverableError, type Job, type JobsOptions, type JobType } from "bullmq";
import { ApiError } from "../api/errors.js";
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

export async function toEasyMQJob(job: Job): Promise<EasyMQJob> {
  const state = await job.getState();
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
    execution,
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
    await this.catalog.register(opts.queue);
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
    const window = offset + limit;
    const seen = new Set<string>();
    const merged: Job[] = [];
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
    const page = merged.slice(offset, offset + limit);
    const mapped = await Promise.all(page.map((job) => toEasyMQJob(job)));
    return {
      jobs: mapped,
      offset,
      limit,
      nextOffset: mapped.length === limit ? offset + limit : null,
    };
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const queue = this.queues.getQueue(queueName);
    const job = await queue.getJob(jobId);
    if (!job) throw ApiError.notFound("job", jobId, queueName);
    const removed = await queue.remove(jobId);
    if (removed === 0) {
      throw new ApiError("CONFLICT", `Job '${jobId}' is currently locked and cannot be removed.`, {
        resource: { type: "job", id: jobId, queue: queueName },
      });
    }
  }

  /** Manually retry a finished (failed or completed) job. */
  async retryJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    const state = await job.getState();
    if (state !== "failed" && state !== "completed") {
      throw new ApiError(
        "CONFLICT",
        `Only failed or completed jobs can be retried (job '${jobId}' is ${state}).`,
        { resource: { type: "job", id: jobId, queue: queueName } },
      );
    }
    await job.retry(state);
    return toEasyMQJob(job);
  }

  /** Promote a delayed job so it runs as soon as possible. */
  async promoteJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    try {
      await job.promote();
    } catch (err) {
      throw new ApiError(
        "CONFLICT",
        `Job '${jobId}' cannot be promoted (only delayed jobs can be promoted).`,
        { resource: { type: "job", id: jobId, queue: queueName }, cause: err },
      );
    }
    return toEasyMQJob(job);
  }

  /** Change the delay of a delayed job. */
  async changeJobDelay(queueName: string, jobId: string, delayMs: number): Promise<EasyMQJob> {
    const job = await this.requireJob(queueName, jobId);
    try {
      await job.changeDelay(delayMs);
    } catch (err) {
      throw new ApiError(
        "CONFLICT",
        `Delay of job '${jobId}' cannot be changed (only delayed jobs support this).`,
        { resource: { type: "job", id: jobId, queue: queueName }, cause: err },
      );
    }
    return toEasyMQJob(job);
  }

  async cancelJob(queueName: string, jobId: string): Promise<EasyMQJob> {
    if (!this.canceller) {
      throw ApiError.internal("Cancellation support is not configured.");
    }
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    await this.canceller.requestCancellation(queueName, jobId);
    return this.getJob(queueName, jobId);
  }

  private async requireJob(queueName: string, jobId: string): Promise<Job> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const job = await this.queues.getQueue(queueName).getJob(jobId);
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
    const message = err instanceof Error ? err.message : String(err);
    if (/deduplication/i.test(message)) {
      return new ApiError("CONFLICT", `Job deduplication conflict: ${message}`, {
        resource: { type: "queue", id: queue },
        cause: err,
      });
    }
    return ApiError.internal(`Failed to create job: ${message}`, err);
  }
}

/** Build the terminal failure for a cancelled attempt (never retried). */
export function cancelledError(): UnrecoverableError {
  return new UnrecoverableError(CANCELLATION_FAILED_REASON);
}
