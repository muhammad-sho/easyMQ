import { UnrecoverableError, type Job } from "bullmq";
import type { Logger } from "../infrastructure/logging/logger.js";
import { CANCELLATION_FAILED_REASON, type StoredJobData } from "../jobs/job-types.js";
import { parseStoredData } from "../jobs/job-service.js";
import {
  ExecutionAbortedError,
  type ExecutionResult,
  type Executor,
  type JobExecutionContext,
} from "../executors/executor.js";
import { ExecutorError } from "../executors/http-executor.js";
import type { CancellationCoordinator } from "./cancellation.js";

/** Executor failures that must NOT be retried (stable, deterministic). */
const UNRECOVERABLE_EXECUTOR_CODES = new Set([
  "SSRF_BLOCKED",
  "REDIRECT_LIMIT_EXCEEDED",
  "RESPONSE_TOO_LARGE",
]);

export interface JobProcessorOptions {
  executors: Executor[];
}

/**
 * Translates BullMQ worker invocations into executor calls.
 *
 * Per attempt:
 *  1. Check the distributed cancellation marker for this exact job
 *     generation + attempt.
 *  2. Run the matching executor with BullMQ's AbortSignal.
 *  3. Map outcomes to easyMQ error semantics (cancelled => failed with
 *     a stable reason and no retry; a completed attempt keeps its result
 *     even if a cancellation marker lands afterwards).
 */
export class JobProcessor {
  private readonly executors: Map<string, Executor>;

  constructor(
    options: JobProcessorOptions,
    private readonly cancellation: CancellationCoordinator,
    private readonly logger?: Logger,
  ) {
    this.executors = new Map(options.executors.map((e) => [e.type, e]));
  }

  handler(queueName: string) {
    return async (
      job: Job<StoredJobData>,
      _token?: string,
      signal?: AbortSignal,
    ): Promise<ExecutionResult> => {
      const jobId = job.id;
      if (jobId === undefined) {
        throw new UnrecoverableError("easymq:invalid-job-data");
      }
      const log = this.logger?.child({ queue: queueName, jobId });

      const { payload, execution } = parseStoredData(job.data);
      if (!execution) {
        throw new UnrecoverableError("easymq:invalid-job-data");
      }

      // Pre-attempt cancellation check (authoritative for this attempt).
      // Identity = immutable creation timestamp + attempt index, so a
      // marker for another generation or attempt never matches.
      if (await this.cancellation.isCancelled(queueName, jobId, job.timestamp, job.attemptsMade)) {
        await this.cancellation.clearMarkerIfMatch(
          queueName,
          jobId,
          job.timestamp,
          job.attemptsMade,
        );
        log?.info({ event: "job-cancelled" }, "Job attempt was cancelled");
        throw new UnrecoverableError(CANCELLATION_FAILED_REASON);
      }

      const executor = this.executors.get(execution.type);
      if (!executor) {
        throw new UnrecoverableError(`easymq:unsupported-execution-type:${execution.type}`);
      }

      const ctx: JobExecutionContext = {
        queue: queueName,
        jobId,
        attemptsMade: job.attemptsMade,
        payload: payload ?? null,
        execution,
        signal: signal ?? new AbortController().signal,
      };

      try {
        const result = await executor.execute(ctx);
        log?.info({ event: "job-executed", statusCode: result.statusCode }, "Job attempt executed");
        return result;
      } catch (err) {
        throw await this.mapExecutionError(
          err,
          queueName,
          jobId,
          job.timestamp,
          job.attemptsMade,
          log,
        );
      }
    };
  }

  private async mapExecutionError(
    err: unknown,
    queue: string,
    jobId: string,
    jobTimestampMs: number,
    attemptsMade: number,
    log: Logger | undefined,
  ): Promise<Error> {
    if (err instanceof UnrecoverableError) return err;

    if (err instanceof ExecutionAbortedError) {
      // Aborted mid-attempt: if the marker still identifies this exact
      // generation + attempt, the distributed cancellation took effect.
      // Otherwise (e.g. shutdown) fail retryably so BullMQ can recover.
      if (await this.cancellation.isCancelled(queue, jobId, jobTimestampMs, attemptsMade)) {
        await this.cancellation.clearMarkerIfMatch(queue, jobId, jobTimestampMs, attemptsMade);
        log?.info({ event: "job-cancelled" }, "Job attempt aborted by cancellation");
        return new UnrecoverableError(CANCELLATION_FAILED_REASON);
      }
      return new Error("easymq:execution-aborted");
    }

    if (err instanceof ExecutorError) {
      const message = `[${err.code}] ${err.message}`;
      if (UNRECOVERABLE_EXECUTOR_CODES.has(err.code)) {
        log?.warn({ event: "job-failed-unrecoverable", code: err.code }, message);
        return new UnrecoverableError(message);
      }
      return new Error(message);
    }

    return err instanceof Error ? err : new Error(String(err));
  }
}
