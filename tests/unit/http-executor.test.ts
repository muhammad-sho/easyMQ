import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ExecutionAbortedError, type JobExecutionContext } from "../../src/executors/executor.js";
import { HttpExecutor, redactUrlForLogging } from "../../src/executors/http-executor.js";

const OPTIONS = {
  timeoutMs: 5000,
  maxResponseBytes: 1_048_576,
  maxRedirects: 5,
  allowPrivateNetwork: true,
};

function ctxFor(url: string, overrides: Partial<JobExecutionContext> = {}): JobExecutionContext {
  return {
    queue: "q",
    jobId: "job-1",
    attemptsMade: 0,
    payload: null,
    execution: { type: "http", url },
    signal: new AbortController().signal,
    ...overrides,
  };
}

let server: Server;
let baseUrl = "";

beforeAll(async () => {
  server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url ?? "/", "http://x");
    if (url.pathname === "/echo") {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      req.on("end", () => {
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            method: req.method,
            headers: req.headers,
            body,
          }),
        );
      });
      return;
    }
    if (url.pathname === "/slow") {
      res.on("error", () => undefined);
      const timer = setTimeout(() => {
        try {
          res.end("slow-response");
        } catch {
          // client already gone
        }
      }, 500);
      req.on("close", () => clearTimeout(timer));
      return;
    }
    if (url.pathname === "/redirect") {
      res.writeHead(302, { location: "/echo" });
      res.end();
      return;
    }
    if (url.pathname === "/redirect-auth") {
      res.writeHead(302, { location: `${globalThis.__CROSS_ORIGIN__ ?? baseUrl}/echo` });
      res.end();
      return;
    }
    if (url.pathname === "/loop") {
      res.writeHead(302, { location: "/loop" });
      res.end();
      return;
    }
    if (url.pathname === "/big") {
      res.end("x".repeat(100_000));
      return;
    }
    res.end("ok");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${String(address.port)}`;
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((err) => (err ? reject(err) : resolve())),
  );
});

describe("HttpExecutor", () => {
  it("performs GET and POST requests and returns the response", async () => {
    const executor = new HttpExecutor(OPTIONS);
    const result = await executor.execute(ctxFor(`${baseUrl}/echo`));
    expect(result.statusCode).toBe(200);
    expect(result.body).toContain("GET");

    const post = await executor.execute({
      ...ctxFor(`${baseUrl}/echo`),
      execution: {
        type: "http",
        url: `${baseUrl}/echo`,
        method: "POST",
        headers: { "x-test": "yes" },
        body: { hello: "world" },
      },
    });
    expect(post.statusCode).toBe(200);
    expect(post.body).toContain("POST");
    expect(post.body).toContain("x-test");
    expect(post.body).toContain("hello");
    expect(post.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("enforces request timeouts", async () => {
    const executor = new HttpExecutor({ ...OPTIONS, timeoutMs: 100 });
    await expect(executor.execute(ctxFor(`${baseUrl}/slow`))).rejects.toMatchObject({
      name: "ExecutorError",
      code: "EXECUTOR_TIMEOUT",
    });
  });

  it("propagates aborts as ExecutionAbortedError", async () => {
    const executor = new HttpExecutor(OPTIONS);
    const controller = new AbortController();
    const pending = executor.execute(ctxFor(`${baseUrl}/slow`, { signal: controller.signal }));
    controller.abort();
    await expect(pending).rejects.toBeInstanceOf(ExecutionAbortedError);
  });

  it("follows same-origin redirects", async () => {
    const executor = new HttpExecutor(OPTIONS);
    const result = await executor.execute(ctxFor(`${baseUrl}/redirect`));
    expect(result.statusCode).toBe(200);
  });

  it("enforces the redirect limit", async () => {
    const executor = new HttpExecutor({ ...OPTIONS, maxRedirects: 2 });
    await expect(executor.execute(ctxFor(`${baseUrl}/loop`))).rejects.toMatchObject({
      code: "REDIRECT_LIMIT_EXCEEDED",
    });
  });

  it("strips credentials on cross-origin redirects", async () => {
    const second = createServer((req: IncomingMessage, res: ServerResponse) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ authorization: req.headers.authorization ?? null }));
    });
    await new Promise<void>((resolve) => second.listen(0, "127.0.0.1", resolve));
    const other = `http://127.0.0.1:${String((second.address() as AddressInfo).port)}`;
    globalThis.__CROSS_ORIGIN__ = other;
    try {
      const executor = new HttpExecutor(OPTIONS);
      const result = await executor.execute({
        ...ctxFor(`${baseUrl}/redirect-auth`),
        execution: {
          type: "http",
          url: `${baseUrl}/redirect-auth`,
          headers: { authorization: "Bearer secret" },
        },
      });
      expect(result.body).toContain('"authorization":null');
    } finally {
      globalThis.__CROSS_ORIGIN__ = undefined;
      await new Promise<void>((resolve, reject) =>
        second.close((err) => (err ? reject(err) : resolve())),
      );
    }
  });

  it("enforces the response-size limit", async () => {
    const executor = new HttpExecutor({ ...OPTIONS, maxResponseBytes: 1024 });
    await expect(executor.execute(ctxFor(`${baseUrl}/big`))).rejects.toMatchObject({
      code: "RESPONSE_TOO_LARGE",
    });
  });

  it("rejects hop-by-hop headers and unsupported protocols", async () => {
    const executor = new HttpExecutor(OPTIONS);
    await expect(
      executor.execute({
        ...ctxFor(`${baseUrl}/echo`),
        execution: { type: "http", url: `${baseUrl}/echo`, headers: { connection: "close" } },
      }),
    ).rejects.toMatchObject({ name: "ExecutorError" });
    await expect(executor.execute(ctxFor("ftp://example.com/x"))).rejects.toMatchObject({
      name: "ExecutorError",
    });
  });

  it("blocks unsafe destinations by default (SSRF protection)", async () => {
    const locked = new HttpExecutor({ ...OPTIONS, allowPrivateNetwork: false });
    await expect(locked.execute(ctxFor(`${baseUrl}/echo`))).rejects.toMatchObject({
      code: "SSRF_BLOCKED",
    });
    // Documentation/reserved IPs are blocked even with private access on.
    const open = new HttpExecutor(OPTIONS);
    await expect(open.execute(ctxFor("http://192.0.2.1/"))).rejects.toMatchObject({
      code: "SSRF_BLOCKED",
    });
    // Metadata-service IPs are blocked even with private access on.
    await expect(open.execute(ctxFor("http://169.254.169.254/"))).rejects.toMatchObject({
      code: "SSRF_BLOCKED",
    });
  });
});

describe("redactUrlForLogging", () => {
  it("redacts userinfo credentials", () => {
    expect(redactUrlForLogging("https://user:pass@example.com/x")).toBe(
      "https://***@example.com/x",
    );
    expect(redactUrlForLogging("https://example.com/x")).toBe("https://example.com/x");
    expect(redactUrlForLogging("not a url")).toBe("[unparseable-url]");
  });
});

declare global {
  var __CROSS_ORIGIN__: string | undefined;
}
