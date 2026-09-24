import { UnrecoverableError, type Job } from "bullmq";
import { describe, expect, it, vi } from "vitest";
import {
  ExecutionAbortedError,
  type ExecutionResult,
  type Executor,
} from "../../src/executors/executor.js";
import { ExecutorError } from "../../src/executors/http-executor.js";
import type { StoredJobData } from "../../src/jobs/job-types.js";
import { JobProcessor } from "../../src/workers/job-processor.js";
import type { CancellationCoordinator } from "../../src/workers/cancellation.js";

const EXECUTION = { type: "http" as const, url: "https://example.com/hook" };

function fakeJob(overrides: Partial<Job<StoredJobData>> = {}): Job<StoredJobData> {
  return {
    id: "job-1",
    attemptsMade: 0,
    timestamp: 1_700_000_000_000,
    data: { version: 1, payload: { a: 1 }, execution: EXECUTION },
    ...overrides,
  } as Job<StoredJobData>;
}

function successExecutor(): Executor & { executed: () => number } {
  let calls = 0;
  const executor: Executor = {
    type: "http",
    execute: (): Promise<ExecutionResult> => {
      calls += 1;
      return Promise.resolve({
        statusCode: 200,
        headers: {},
        body: "ok",
        bodyTruncated: false,
        durationMs: 1,
      });
    },
  };
  return Object.assign(executor, { executed: () => calls });
}

function cancellationStub(cancelled: boolean): CancellationCoordinator {
  return {
    isCancelled: vi.fn().mockResolvedValue(cancelled),
    clearMarkerIfMatch: vi.fn().mockResolvedValue(true),
  } as unknown as CancellationCoordinator;
}

describe("JobProcessor", () => {
  it("executes the job with the matching executor", async () => {
    const executor = successExecutor();
    const processor = new JobProcessor({ executors: [executor] }, cancellationStub(false));
    const result = await processor.handler("q")(fakeJob());
    expect(result.statusCode).toBe(200);
  });

  it("refuses cancelled attempts without retry", async () => {
    const executor = successExecutor();
    const cancellation = cancellationStub(true);
    const processor = new JobProcessor({ executors: [executor] }, cancellation);
    await expect(processor.handler("q")(fakeJob())).rejects.toMatchObject({
      name: "UnrecoverableError",
      message: "easymq:cancelled",
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- vi.fn() has no this-scoping hazard
    expect(cancellation.clearMarkerIfMatch).toHaveBeenCalledWith(
      "q",
      "job-1",
      1_700_000_000_000,
      0,
    );
  });

  it("maps mid-attempt aborts with a marker to cancellation", async () => {
    const aborting: Executor = {
      type: "http",
      execute: () => Promise.reject<ExecutionResult>(new ExecutionAbortedError()),
    };
    const cancellation = cancellationStub(true);
    const processor = new JobProcessor({ executors: [aborting] }, cancellation);
    await expect(processor.handler("q")(fakeJob())).rejects.toMatchObject({
      name: "UnrecoverableError",
      message: "easymq:cancelled",
    });
  });

  it("fails retryably on aborts without a marker", async () => {
    const aborting: Executor = {
      type: "http",
      execute: () => Promise.reject<ExecutionResult>(new ExecutionAbortedError()),
    };
    const processor = new JobProcessor({ executors: [aborting] }, cancellationStub(false));
    const err = await processor
      .handler("q")(fakeJob())
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(UnrecoverableError);
  });

  it("marks deterministic executor failures as unrecoverable", async () => {
    const failing: Executor = {
      type: "http",
      execute: () => Promise.reject<ExecutionResult>(new ExecutorError("SSRF_BLOCKED", "Blocked.")),
    };
    const processor = new JobProcessor({ executors: [failing] }, cancellationStub(false));
    await expect(processor.handler("q")(fakeJob())).rejects.toBeInstanceOf(UnrecoverableError);
  });

  it("keeps transient executor failures retryable with stable codes", async () => {
    const failing: Executor = {
      type: "http",
      execute: () =>
        Promise.reject<ExecutionResult>(new ExecutorError("EXECUTOR_TIMEOUT", "Timed out.")),
    };
    const processor = new JobProcessor({ executors: [failing] }, cancellationStub(false));
    const err = (await processor
      .handler("q")(fakeJob())
      .catch((e: unknown) => e)) as Error;
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(UnrecoverableError);
    expect(err.message).toContain("[EXECUTOR_TIMEOUT]");
  });

  it("fails permanently on unknown execution types and malformed data", async () => {
    const processor = new JobProcessor({ executors: [successExecutor()] }, cancellationStub(false));
    await expect(
      processor.handler("q")(
        fakeJob({
          data: {
            version: 1,
            payload: null,
            execution: { type: "smtp" },
          } as unknown as StoredJobData,
        }),
      ),
    ).rejects.toBeInstanceOf(UnrecoverableError);
  });
});
