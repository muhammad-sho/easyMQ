import { describe, expect, it } from "vitest";
import { ApiError, classifyBackendError, isConnectionError } from "../../src/api/errors.js";

function namedError(name: string, message = "backend blew up"): Error {
  const err = new Error(message);
  err.name = name;
  return err;
}

function codedError(code: string, message = "connect failed"): Error & { code: string } {
  const err = new Error(message) as Error & { code: string };
  (err as { code?: string }).code = code;
  return err;
}

describe("isConnectionError", () => {
  it("recognizes backend connection failures", () => {
    expect(isConnectionError(namedError("MaxRetriesPerRequestError"))).toBe(true);
    expect(isConnectionError(namedError("ConnectionClosedError"))).toBe(true);
    expect(isConnectionError(namedError("ClusterAllFailedError"))).toBe(true);
    expect(isConnectionError(codedError("ECONNREFUSED"))).toBe(true);
    expect(isConnectionError(codedError("ENOTFOUND"))).toBe(true);
    expect(isConnectionError(namedError("Error", "Connection is closed"))).toBe(true);
    expect(isConnectionError(namedError("Error", "Stream isn't writeable"))).toBe(true);
    expect(isConnectionError(ApiError.serviceUnavailable("down"))).toBe(true);
  });

  it("rejects ordinary failures", () => {
    expect(isConnectionError(new Error("Delayed job cannot be promoted"))).toBe(false);
    expect(isConnectionError(namedError("DelayedError"))).toBe(false);
    expect(isConnectionError(ApiError.validation("bad input"))).toBe(false);
    expect(isConnectionError("just a string")).toBe(false);
    expect(isConnectionError(null)).toBe(false);
  });
});

describe("classifyBackendError", () => {
  it("passes ApiError values through untouched", () => {
    const original = ApiError.notFound("job", "1", "q");
    expect(classifyBackendError(original, "job creation")).toBe(original);
  });

  it("maps connection failures to service-unavailable without internals", () => {
    const err = classifyBackendError(
      codedError("ECONNREFUSED", "connect ECONNREFUSED 10.0.0.9:6380"),
      "job creation",
    );
    expect(err.code).toBe("SERVICE_UNAVAILABLE");
    expect(err.statusCode).toBe(503);
    const body = JSON.stringify(err.toBody());
    expect(body).not.toContain("10.0.0.9");
    expect(body).not.toContain("ECONNREFUSED");
  });

  it("maps unexpected failures to internal error without backend wording", () => {
    const raw = new Error("Missing lock for job 42. failed to run clean job script");
    const err = classifyBackendError(raw, "schedule upsert");
    expect(err.code).toBe("INTERNAL_ERROR");
    expect(err.statusCode).toBe(500);
    const body = JSON.stringify(err.toBody());
    expect(body).not.toContain("Missing lock");
    expect(body).not.toContain("clean job script");
    // The original stays attached server-side for structured logs.
    expect(err.cause).toBe(raw);
  });

  it("keeps public messages stable", () => {
    expect(classifyBackendError(codedError("ECONNREFUSED"), "job creation").message).toBe(
      "Backend temporarily unavailable during job creation.",
    );
    expect(classifyBackendError(new Error("x"), "job creation").message).toBe(
      "Unexpected backend failure during job creation.",
    );
  });
});
