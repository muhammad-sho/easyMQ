import type { Execution, Json } from "../jobs/job-types.js";

/** Context for a single execution attempt. */
export interface JobExecutionContext {
  queue: string;
  jobId: string;
  attemptsMade: number;
  payload: Json;
  execution: Execution;
  /** Aborted on timeout or distributed cancellation. */
  signal: AbortSignal;
}

/** Normalized result returned by an executor. */
export interface ExecutionResult {
  statusCode: number;
  headers: Record<string, string>;
  body: string | null;
  bodyTruncated: boolean;
  durationMs: number;
}

/**
 * Small executor contract: `execute(job, signal) -> result`.
 * New executor types plug in here without touching queue infrastructure.
 */
export interface Executor {
  readonly type: Execution["type"];
  execute(ctx: JobExecutionContext): Promise<ExecutionResult>;
}

/** Thrown when the AbortSignal fires (timeout or cancellation). */
export class ExecutionAbortedError extends Error {
  constructor(message = "Execution aborted.") {
    super(message);
    this.name = "ExecutionAbortedError";
  }
}
