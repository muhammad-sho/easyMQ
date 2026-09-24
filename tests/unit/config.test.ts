import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config/env.js";

describe("loadConfig", () => {
  it("applies documented defaults", () => {
    const config = loadConfig({ AUTH_DISABLED: "true" });
    expect(config.redisUrl).toBe("redis://127.0.0.1:6379");
    expect(config.redisKeyPrefix).toBe("easymq");
    expect(config.apiHost).toBe("0.0.0.0");
    expect(config.apiPort).toBe(3000);
    expect(config.appRole).toBe("both");
    expect(config.workerConcurrency).toBe(10);
    expect(config.httpTimeoutMs).toBe(30_000);
    expect(config.httpMaxResponseBytes).toBe(1_048_576);
    expect(config.httpMaxRedirects).toBe(5);
    expect(config.httpAllowPrivateNetwork).toBe(false);
    expect(config.cancellationTtlSeconds).toBe(300);
    expect(config.shutdownTimeoutMs).toBe(30_000);
    expect(config.logLevel).toBe("info");
    expect(config.defaultAttempts).toBe(3);
    expect(config.defaultBackoffType).toBe("exponential");
    expect(config.defaultBackoffDelayMs).toBe(5000);
    expect(config.defaultRemoveOnCompleteCount).toBe(1000);
    expect(config.defaultRemoveOnFailCount).toBe(5000);
    expect(config.pageDefaultLimit).toBe(50);
    expect(config.pageMaxLimit).toBe(200);
  });

  it("parses numbers, booleans and enums from strings", () => {
    const config = loadConfig({
      API_PORT: "8080",
      APP_ROLE: "worker",
      WORKER_CONCURRENCY: "4",
      HTTP_ALLOW_PRIVATE_NETWORK: "yes",
      LOG_LEVEL: "debug",
      DEFAULT_BACKOFF_TYPE: "fixed",
      AUTH_DISABLED: "1",
    });
    expect(config.apiPort).toBe(8080);
    expect(config.appRole).toBe("worker");
    expect(config.workerConcurrency).toBe(4);
    expect(config.httpAllowPrivateNetwork).toBe(true);
    expect(config.logLevel).toBe("debug");
    expect(config.defaultBackoffType).toBe("fixed");
  });

  it("requires API_TOKEN when serving the API without AUTH_DISABLED", () => {
    expect(() => loadConfig({})).toThrow(/API_TOKEN/);
    expect(() => loadConfig({ APP_ROLE: "api" })).toThrow(/API_TOKEN/);
    // Worker-only role does not serve the API, so no token is needed.
    expect(loadConfig({ APP_ROLE: "worker" }).apiToken).toBeUndefined();
    expect(loadConfig({ API_TOKEN: "secret" }).apiToken).toBe("secret");
  });

  it("rejects invalid numbers, booleans and enums with clear errors", () => {
    expect(() => loadConfig({ AUTH_DISABLED: "true", API_PORT: "abc" })).toThrow(/API_PORT/);
    expect(() => loadConfig({ AUTH_DISABLED: "maybe" })).toThrow(/boolean/);
    expect(() => loadConfig({ AUTH_DISABLED: "true", APP_ROLE: "nope" })).toThrow();
    expect(() => loadConfig({ AUTH_DISABLED: "true", API_PORT: "-1" })).toThrow();
  });

  it("rejects inconsistent pagination defaults", () => {
    expect(() =>
      loadConfig({
        AUTH_DISABLED: "true",
        PAGE_DEFAULT_LIMIT: "100",
        PAGE_MAX_LIMIT: "50",
      }),
    ).toThrow(/PAGE_DEFAULT_LIMIT/);
  });
});
