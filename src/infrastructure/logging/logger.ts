import pino, { type Logger } from "pino";
import type { AppRole, LogLevel } from "../../config/schema.js";

export interface CreateLoggerOptions {
  level?: LogLevel;
  role?: AppRole;
  pretty?: boolean;
}

/**
 * Sensitive paths that must never appear in logs in clear text.
 * Covers API tokens, auth headers, cookies and generic credential fields.
 */
const REDACT_PATHS = [
  "apiToken",
  "token",
  "password",
  "passwd",
  "*.password",
  "*.apiToken",
  "*.token",
  "req.headers.authorization",
  "req.headers.cookie",
  "res.headers.authorization",
  "headers.authorization",
  "headers.cookie",
  "authorization",
  "cookie",
  "body.password",
  "body.token",
  "execution.headers.authorization",
  "headers.*authorization*",
];

/**
 * Create the root structured JSON logger.
 * Service/role base fields are attached to every line.
 */
export function createLogger(options: CreateLoggerOptions = {}): Logger {
  const { level = "info", role, pretty = false } = options;
  return pino({
    level,
    base: {
      service: "easymq",
      ...(role !== undefined ? { role } : {}),
    },
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: {
      paths: REDACT_PATHS,
      censor: "[Redacted]",
    },
    ...(pretty
      ? {
          transport: {
            target: "pino-pretty",
            options: { colorize: true, singleLine: true },
          },
        }
      : {}),
  });
}

/** Create a child logger carrying queue/job context. */
export function childLogger(
  log: Logger,
  bindings: Record<string, string | number | boolean>,
): Logger {
  return log.child(bindings);
}

export type { Logger };
