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

  static notFound(
    type: string,
    id: string,
    queue?: string,
  ): ApiError {
    const resource: ApiErrorResource =
      queue !== undefined ? { type, id, queue } : { type, id };
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
  return ApiError.internal(
    "Internal server error.",
    err instanceof Error ? err : undefined,
  );
}

export function statusForCode(code: EasyMQErrorCode): number {
  return STATUS_BY_CODE[code];
}

export { STATUS_BY_CODE };
