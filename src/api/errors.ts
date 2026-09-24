import type { EasyMQErrorCode } from "../jobs/job-types.js";

const STATUS_BY_CODE: Record<EasyMQErrorCode, number> = {
  VALIDATION_ERROR: 400,
  UNAUTHENTICATED: 401,
  NOT_FOUND: 404,
  CONFLICT: 409,
  JOB_NOT_ACTIVE: 409,
  CANCELLED: 409,
  EXECUTOR_ERROR: 500,
  EXECUTOR_TIMEOUT: 504,
  SSRF_BLOCKED: 500,
  REDIRECT_LIMIT_EXCEEDED: 500,
  RESPONSE_TOO_LARGE: 500,
  SERVICE_UNAVAILABLE: 503,
  INTERNAL_ERROR: 500,
};

export interface ApiErrorResource {
  type: string;
  id?: string;
  queue?: string;
}

export interface ApiErrorBody {
  error: {
    code: EasyMQErrorCode;
    message: string;
    resource?: ApiErrorResource;
    details?: unknown;
  };
}

/**
 * Stable easyMQ API error. Services throw these; the Fastify error
 * handler translates them to HTTP responses. Raw BullMQ/Redis errors
 * must never reach the public API contract.
 */
export class ApiError extends Error {
  readonly code: EasyMQErrorCode;
  readonly statusCode: number;
  readonly resource: ApiErrorResource | undefined;
  readonly details: unknown;

  constructor(
    code: EasyMQErrorCode,
    message: string,
    options: {
      statusCode?: number;
      resource?: ApiErrorResource | undefined;
      details?: unknown;
      cause?: unknown;
    } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : {});
    this.name = "ApiError";
    this.code = code;
    this.statusCode = options.statusCode ?? STATUS_BY_CODE[code];
    this.resource = options.resource;
    this.details = options.details;
  }

  toBody(): ApiErrorBody {
    return {
      error: {
        code: this.code,
        message: this.message,
        ...(this.resource !== undefined ? { resource: this.resource } : {}),
        ...(this.details !== undefined ? { details: this.details } : {}),
      },
    };
  }

  static notFound(type: string, id: string, queue?: string): ApiError {
    const resource: ApiErrorResource = queue !== undefined ? { type, id, queue } : { type, id };
    return new ApiError("NOT_FOUND", `${type} '${id}' not found.`, {
      resource,
    });
  }

  static validation(message: string, details?: unknown): ApiError {
    return new ApiError("VALIDATION_ERROR", message, { details });
  }

  static conflict(
    code: Extract<EasyMQErrorCode, "CONFLICT" | "JOB_NOT_ACTIVE" | "CANCELLED">,
    message: string,
    resource?: ApiErrorResource,
  ): ApiError {
    return new ApiError(code, message, { resource });
  }

  static serviceUnavailable(message: string, cause?: unknown): ApiError {
    return new ApiError("SERVICE_UNAVAILABLE", message, { cause });
  }

  static internal(message: string, cause?: unknown): ApiError {
    return new ApiError("INTERNAL_ERROR", message, { cause });
  }
}

/** Map any thrown value to an ApiError (never leak internals). */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  return ApiError.internal("Internal server error.", err instanceof Error ? err : undefined);
}

export function statusForCode(code: EasyMQErrorCode): number {
  return STATUS_BY_CODE[code];
}

/**
 * Error names that always mean "the backend could not be reached".
 * Matched by name so callers never depend on deep ioredis import paths.
 */
const CONNECTION_ERROR_NAMES = new Set([
  "MaxRetriesPerRequestError",
  "ConnectionClosedError",
  "ClusterAllFailedError",
  "ConnectionNotReadyError",
]);

/** Node.js syscall codes that always mean "the backend could not be reached". */
const CONNECTION_ERROR_CODES = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ETIMEDOUT",
  "EPIPE",
  "ECONNRESET",
  "ENETUNREACH",
  "EHOSTUNREACH",
]);

/** Message fragments (backend wording) that mean "the backend is down". */
const CONNECTION_MESSAGE_PATTERNS = [
  /connection is closed/i,
  /connection lost/i,
  /stream isn't writeable/i,
  /offline queue/i,
  /max retries per request/i,
];

/**
 * True when the error means Redis/BullMQ could not be reached.
 * Used to map operational outages to SERVICE_UNAVAILABLE without
 * leaking backend wording, hosts, or Lua internals into the API.
 */
export function isConnectionError(err: unknown): boolean {
  if (err instanceof ApiError) return err.code === "SERVICE_UNAVAILABLE";
  if (typeof err !== "object" || err === null) return false;
  const record = err as { name?: unknown; code?: unknown; message?: unknown };
  if (typeof record.name === "string" && CONNECTION_ERROR_NAMES.has(record.name)) return true;
  if (typeof record.code === "string" && CONNECTION_ERROR_CODES.has(record.code)) return true;
  if (typeof record.message === "string") {
    return CONNECTION_MESSAGE_PATTERNS.some((pattern) => pattern.test(record.message as string));
  }
  return false;
}

/**
 * Map an unknown backend failure to a stable API error:
 * connection problems -> 503, everything else -> 500.
 * ApiError values pass through untouched. The original error is kept
 * as `cause` for server-side logs; public messages never interpolate
 * backend text.
 */
export function classifyBackendError(err: unknown, operation: string): ApiError {
  if (err instanceof ApiError) return err;
  if (isConnectionError(err)) {
    return ApiError.serviceUnavailable(`Backend temporarily unavailable during ${operation}.`, err);
  }
  return ApiError.internal(`Unexpected backend failure during ${operation}.`, err);
}

export { STATUS_BY_CODE };
