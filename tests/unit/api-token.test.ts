import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config/env.js";
import { resolveApiAuth } from "../../src/config/api-token.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import { uniquePrefix, REDIS_URL } from "../integration/helpers.js";
import { RedisConnectionManager } from "../../src/infrastructure/redis/connection-manager.js";

describe("resolveApiAuth", () => {
  it("keeps an explicit API_TOKEN", async () => {
    const connections = new RedisConnectionManager(REDIS_URL);
    const redis = connections.getShared();
    try {
      await connections.waitUntilReady();
      const config = loadConfig({ API_TOKEN: "pinned-secret" });
      const result = await resolveApiAuth(config, redis);
      expect(result.source).toBe("env");
      expect(result.config.apiToken).toBe("pinned-secret");
    } finally {
      await connections.closeAll();
    }
  });

  it("skips resolution for worker role and AUTH_DISABLED", async () => {
    const connections = new RedisConnectionManager(REDIS_URL);
    const redis = connections.getShared();
    try {
      await connections.waitUntilReady();
      const worker = await resolveApiAuth(loadConfig({ APP_ROLE: "worker" }), redis);
      expect(worker.source).toBe("disabled");
      expect(worker.config.apiToken).toBeUndefined();

      const open = await resolveApiAuth(loadConfig({ AUTH_DISABLED: "true" }), redis);
      expect(open.source).toBe("disabled");
      expect(open.config.apiToken).toBeUndefined();
    } finally {
      await connections.closeAll();
    }
  });

  it("generates a token once and reuses it from Redis", async () => {
    const prefix = uniquePrefix("auth-gen");
    const connections = new RedisConnectionManager(REDIS_URL);
    const redis = connections.getShared();
    const logger = createLogger({ level: "silent" });
    try {
      await connections.waitUntilReady();
      const base = loadConfig({ REDIS_KEY_PREFIX: prefix });
      const first = await resolveApiAuth(base, redis, logger);
      expect(first.source).toBe("generated");
      expect(first.config.apiToken).toMatch(/^[A-Za-z0-9_-]{32,}$/);

      const stored = await redis.get(`${prefix}:auth:api-token`);
      expect(stored).toBe(first.config.apiToken);

      const second = await resolveApiAuth(loadConfig({ REDIS_KEY_PREFIX: prefix }), redis, logger);
      expect(second.source).toBe("redis");
      expect(second.config.apiToken).toBe(first.config.apiToken);
    } finally {
      await connections.closeAll();
    }
  });
});
