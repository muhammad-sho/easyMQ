import { randomBytes } from "node:crypto";
import type { Redis } from "ioredis";
import type { AppConfig } from "./schema.js";
import type { Logger } from "../infrastructure/logging/logger.js";

export type ApiTokenSource = "env" | "generated" | "redis" | "disabled";

export interface ResolvedApiAuth {
  config: AppConfig;
  source: ApiTokenSource;
}

function servesApi(config: AppConfig): boolean {
  return config.appRole === "api" || config.appRole === "both";
}

function withToken(config: AppConfig, apiToken: string): AppConfig {
  return { ...config, apiToken };
}

/**
 * Ensure the API has a bearer token when auth is enabled.
 *
 * Hierarchy:
 * 1. `API_TOKEN` from the environment (never logged)
 * 2. A deployment-scoped token persisted in Redis (stable across restarts
 *    while the Redis volume lives; unique per deployment — not a shared default)
 * 3. Generate a cryptographically random token on first start (SET NX)
 *
 * Worker-only roles and `AUTH_DISABLED=true` skip resolution.
 */
export async function resolveApiAuth(
  config: AppConfig,
  redis: Redis,
  logger?: Logger,
): Promise<ResolvedApiAuth> {
  if (!servesApi(config) || config.authDisabled) {
    return { config, source: "disabled" };
  }
  if (config.apiToken !== undefined) {
    return { config, source: "env" };
  }

  const key = `${config.redisKeyPrefix}:auth:api-token`;
  const candidate = randomBytes(32).toString("base64url");
  const created = await redis.set(key, candidate, "NX");
  if (created === "OK") {
    logger?.info(
      { event: "api-token-ready", source: "generated" },
      `API token: ${candidate} (auto-generated; set API_TOKEN to override)`,
    );
    return { config: withToken(config, candidate), source: "generated" };
  }

  const existing = await redis.get(key);
  if (existing !== null && existing !== "") {
    logger?.info(
      { event: "api-token-ready", source: "redis" },
      `API token: ${existing} (from Redis; set API_TOKEN to override)`,
    );
    return { config: withToken(config, existing), source: "redis" };
  }

  // Extremely rare race: NX lost but the key vanished before GET.
  const retry = randomBytes(32).toString("base64url");
  const retryCreated = await redis.set(key, retry, "NX");
  const resolved = retryCreated === "OK" ? retry : await redis.get(key);
  if (resolved === null || resolved === "") {
    throw new Error("Failed to initialize API token in Redis.");
  }
  const source = retryCreated === "OK" ? "generated" : "redis";
  logger?.info(
    { event: "api-token-ready", source },
    `API token: ${resolved} (auto-generated; set API_TOKEN to override)`,
  );
  return { config: withToken(config, resolved), source };
}
