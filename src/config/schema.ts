import { z } from "zod";

export const appRoleSchema = z.enum(["api", "worker", "both"]);
export type AppRole = z.infer<typeof appRoleSchema>;

export const logLevelSchema = z.enum([
  "fatal",
  "error",
  "warn",
  "info",
  "debug",
  "trace",
  "silent",
]);
export type LogLevel = z.infer<typeof logLevelSchema>;

export const backoffTypeSchema = z.enum(["fixed", "exponential"]);
export type DefaultBackoffType = z.infer<typeof backoffTypeSchema>;

/**
 * Fully-resolved, validated application configuration.
 * All values have concrete types here — environment parsing
 * (strings -> numbers/booleans) happens in `env.ts` before validation.
 */
export const configSchema = z
  .object({
    redisUrl: z.string().min(1),
    redisKeyPrefix: z.string().min(1),
    apiHost: z.string().min(1),
    apiPort: z.number().int().min(0).max(65535),
    apiToken: z.string().min(1).optional(),
    authDisabled: z.boolean(),
    appRole: appRoleSchema,
    workerConcurrency: z.number().int().min(1).max(1000),
    httpTimeoutMs: z.number().int().min(100).max(600_000),
    httpMaxResponseBytes: z.number().int().min(1024).max(100_000_000),
    httpMaxRedirects: z.number().int().min(0).max(20),
    httpAllowPrivateNetwork: z.boolean(),
    cancellationTtlSeconds: z.number().int().min(10).max(86_400),
    shutdownTimeoutMs: z.number().int().min(0).max(600_000),
    logLevel: logLevelSchema,
    logPretty: z.boolean(),
    defaultAttempts: z.number().int().min(1).max(100),
    defaultBackoffType: backoffTypeSchema,
    defaultBackoffDelayMs: z.number().int().min(0).max(3_600_000),
    defaultRemoveOnCompleteCount: z.number().int().min(0).max(1_000_000),
    defaultRemoveOnFailCount: z.number().int().min(0).max(1_000_000),
    pageDefaultLimit: z.number().int().min(1).max(1000),
    pageMaxLimit: z.number().int().min(1).max(1000),
  })
  .superRefine((cfg, ctx) => {
    const servesApi = cfg.appRole === "api" || cfg.appRole === "both";
    if (servesApi && !cfg.authDisabled && !cfg.apiToken) {
      ctx.addIssue({
        code: "custom",
        path: ["apiToken"],
        message:
          "API_TOKEN is required when serving the API. Set API_TOKEN or explicitly set AUTH_DISABLED=true for local development only.",
      });
    }
    if (cfg.pageDefaultLimit > cfg.pageMaxLimit) {
      ctx.addIssue({
        code: "custom",
        path: ["pageDefaultLimit"],
        message: "PAGE_DEFAULT_LIMIT must not exceed PAGE_MAX_LIMIT.",
      });
    }
  });

export type AppConfig = z.infer<typeof configSchema>;
