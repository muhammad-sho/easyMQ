/**
 * easyMQ public types.
 *
 * These are easyMQ's own stable contracts. BullMQ types must never leak
 * into the public API — translation happens in the service layer.
 */

/** Arbitrary JSON payload supplied by the calling application. */
export type JsonPrimitive = string | number | boolean | null;
export type Json = JsonPrimitive | Json[] | { [key: string]: Json };

/** States easyMQ exposes for jobs (superset of BullMQ's JobState). */
export type EasyMQJobState =
  | "waiting"
  | "active"
  | "completed"
  | "failed"
  | "delayed"
  | "prioritized"
  | "waiting-children"
  | "unknown";

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS";

export interface HttpExecution {
  type: "http";
  url: string;
  method?: HttpMethod | undefined;
  headers?: Record<string, string> | undefined;
  /** Body: JSON values are sent as application/json, strings as-is. */
  body?: Json | string | undefined;
  /** Per-job timeout override (ms). Falls back to HTTP_TIMEOUT_MS. */
  timeoutMs?: number | undefined;
}

/**
 * How easyMQ should execute the job. Union so future executor types
 * (e.g. webhook signing, function calls) can be added without
 * changing the queue architecture.
 */
export type Execution = HttpExecution;

/**
 * API-safe execution view. Keeps useful metadata (type, URL, method, and
 * non-secret options) but never carries header VALUES: those stay
 * server-side where the worker needs them. Only header names are exposed
 * so callers can see which headers were configured.
 */
export interface PublicHttpExecution {
  type: "http";
  url: string;
  method?: HttpMethod | undefined;
  headerNames?: string[] | undefined;
  body?: Json | string | undefined;
  timeoutMs?: number | undefined;
}

export type PublicExecution = PublicHttpExecution;

/** Shape stored as BullMQ job data. `payload` and `execution` stay separate. */
export interface StoredJobData {
  version: 1;
  payload: Json;
  execution: Execution;
}

export interface BackoffOptions {
  type: "fixed" | "exponential";
  delayMs: number;
}

export interface DeduplicationOptions {
  id: string;
  ttlMs?: number | undefined;
  replace?: boolean | undefined;
}

/**
 * Debounce semantics implemented on top of BullMQ deduplication:
 * submitting the same id within `delayMs` replaces the pending job
 * instead of creating a new one.
 */
export interface DebounceOptions {
  id: string;
  delayMs: number;
  replace?: boolean | undefined;
}

export interface RetentionOptions {
  count?: number | undefined;
  ageSeconds?: number | undefined;
}

export interface CreateJobOptions {
  queue: string;
  name?: string | undefined;
  payload?: Json | undefined;
  execution: Execution;
  jobId?: string | undefined;
  delayMs?: number | undefined;
  attempts?: number | undefined;
  backoff?: BackoffOptions | undefined;
  priority?: number | undefined;
  lifo?: boolean | undefined;
  deduplication?: DeduplicationOptions | undefined;
  debounce?: DebounceOptions | undefined;
  removeOnComplete?: RetentionOptions | undefined;
  removeOnFail?: RetentionOptions | undefined;
}

export interface ListJobsOptions {
  queue: string;
  states?: EasyMQJobState[] | undefined;
  offset?: number | undefined;
  limit?: number | undefined;
  asc?: boolean | undefined;
}

export interface EasyMQJob {
  id: string;
  queue: string;
  name: string;
  state: EasyMQJobState;
  payload: Json | null;
  /** Redacted execution view — header values are never returned. */
  execution: PublicExecution | null;
  attemptsMade: number;
  priority: number;
  delayMs: number;
  timestampMs: number;
  processedOnMs: number | null;
  finishedOnMs: number | null;
  failedReason: string | null;
  returnValue: Json | null;
}

export interface QueueCounts {
  counts: Record<string, number>;
}

export interface QueueInfo {
  name: string;
  isPaused: boolean;
  counts: Record<string, number>;
}

export interface ScheduleOptions {
  id: string;
  queue: string;
  name?: string | undefined;
  /** Cron pattern (mutually exclusive with everyMs). */
  pattern?: string | undefined;
  /** Fixed interval in ms (mutually exclusive with pattern). */
  everyMs?: number | undefined;
  timezone?: string | undefined;
  startDateMs?: number | undefined;
  endDateMs?: number | undefined;
  limit?: number | undefined;
  payload?: Json | undefined;
  execution: Execution;
  attempts?: number | undefined;
  backoff?: BackoffOptions | undefined;
  priority?: number | undefined;
  removeOnComplete?: RetentionOptions | undefined;
  removeOnFail?: RetentionOptions | undefined;
}

export interface EasyMQSchedule {
  id: string;
  queue: string;
  name: string;
  pattern: string | null;
  everyMs: number | null;
  timezone: string | null;
  nextRunAtMs: number | null;
  iterationCount: number | null;
  limit: number | null;
  startDateMs: number | null;
  endDateMs: number | null;
}

/** Stable easyMQ error codes (public API contract). */
export const EASYMQ_ERROR_CODES = [
  "VALIDATION_ERROR",
  "UNAUTHENTICATED",
  "NOT_FOUND",
  "CONFLICT",
  "JOB_NOT_ACTIVE",
  "CANCELLED",
  "EXECUTOR_ERROR",
  "EXECUTOR_TIMEOUT",
  "SSRF_BLOCKED",
  "REDIRECT_LIMIT_EXCEEDED",
  "RESPONSE_TOO_LARGE",
  "SERVICE_UNAVAILABLE",
  "INTERNAL_ERROR",
] as const;

export type EasyMQErrorCode = (typeof EASYMQ_ERROR_CODES)[number];

/** Cancellation is surfaced as a failed job with this stable reason. */
export const CANCELLATION_FAILED_REASON = "easymq:cancelled";
