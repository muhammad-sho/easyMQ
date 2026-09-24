import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ExecutionAbortedError, type JobExecutionContext } from "../../src/executors/executor.js";
import {
  HttpExecutor,
  assertSafeTarget,
  classifyAddress,
  redactUrlForLogging,
} from "../../src/executors/http-executor.js";

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

  it("strips all caller headers on cross-origin redirects", async () => {
    const received: Array<Record<string, string | string[] | undefined>> = [];
    const second = createServer((req: IncomingMessage, res: ServerResponse) => {
      received.push({ ...req.headers });
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
          headers: {
            authorization: "Bearer secret",
            cookie: "session=abc",
            "x-api-key": "key-123",
            "x-auth-token": "token-456",
          },
        },
      });
      expect(result.body).toContain('"authorization":null');
      const seen = received[0] ?? {};
      for (const name of ["authorization", "cookie", "x-api-key", "x-auth-token"]) {
        expect(seen[name], `header ${name} must not cross origins`).toBeUndefined();
      }
    } finally {
      globalThis.__CROSS_ORIGIN__ = undefined;
      await new Promise<void>((resolve, reject) =>
        second.close((err) => (err ? reject(err) : resolve())),
      );
    }
  });

  it("retains caller headers on same-origin redirects", async () => {
    const executor = new HttpExecutor(OPTIONS);
    const result = await executor.execute({
      ...ctxFor(`${baseUrl}/redirect`),
      execution: {
        type: "http",
        url: `${baseUrl}/redirect`,
        headers: { "x-api-key": "key-123" },
      },
    });
    expect(result.statusCode).toBe(200);
    expect(result.body).toContain("x-api-key");
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

  it("ignores removed per-job network-policy overrides", async () => {
    // Even if a stale caller submits allowPrivateNetwork, only the
    // operator-side option applies.
    const locked = new HttpExecutor({ ...OPTIONS, allowPrivateNetwork: false });
    const sneaky = {
      ...ctxFor(`${baseUrl}/echo`),
      execution: {
        type: "http",
        url: `${baseUrl}/echo`,
        allowPrivateNetwork: true,
      },
    };
    await expect(
      locked.execute(sneaky as unknown as Parameters<HttpExecutor["execute"]>[0]),
    ).rejects.toMatchObject({ code: "SSRF_BLOCKED" });
  });
});

describe("classifyAddress", () => {
  const cases: Array<{ address: string; blocked: boolean; openBlocked: boolean }> = [
    // IPv4
    { address: "127.0.0.1", blocked: true, openBlocked: false },
    { address: "169.254.10.20", blocked: true, openBlocked: false },
    { address: "169.254.169.254", blocked: true, openBlocked: true },
    { address: "100.100.100.100", blocked: true, openBlocked: true },
    { address: "10.1.2.3", blocked: true, openBlocked: false },
    { address: "172.16.5.4", blocked: true, openBlocked: false },
    { address: "172.31.255.255", blocked: true, openBlocked: false },
    { address: "192.168.1.1", blocked: true, openBlocked: false },
    { address: "0.0.0.0", blocked: true, openBlocked: true },
    { address: "224.0.0.1", blocked: true, openBlocked: true },
    { address: "192.0.2.1", blocked: true, openBlocked: true },
    { address: "100.64.0.1", blocked: true, openBlocked: true },
    { address: "93.184.216.34", blocked: false, openBlocked: false },
    // IPv6
    { address: "::1", blocked: true, openBlocked: false },
    { address: "fe80::1", blocked: true, openBlocked: false },
    { address: "fc00::1", blocked: true, openBlocked: false },
    { address: "fd00::1", blocked: true, openBlocked: false },
    { address: "fd00:ec2::254", blocked: true, openBlocked: true },
    { address: "::", blocked: true, openBlocked: true },
    { address: "ff02::1", blocked: true, openBlocked: true },
    { address: "2001:db8::1", blocked: true, openBlocked: true },
    { address: "2606:4700:4700::1111", blocked: false, openBlocked: false },
    // IPv4-mapped IPv6 follows the embedded IPv4 verdict.
    { address: "::ffff:127.0.0.1", blocked: true, openBlocked: false },
    { address: "::ffff:169.254.169.254", blocked: true, openBlocked: true },
    { address: "::ffff:10.0.0.1", blocked: true, openBlocked: false },
    { address: "::ffff:93.184.216.34", blocked: false, openBlocked: false },
  ];
  for (const { address, blocked, openBlocked } of cases) {
    it(`classifies ${address} (locked=${String(blocked)}, open=${String(openBlocked)})`, () => {
      expect(classifyAddress(address, false) !== null).toBe(blocked);
      expect(classifyAddress(address, true) !== null).toBe(openBlocked);
    });
  }
});

describe("assertSafeTarget", () => {
  it("rejects a hostname when ANY resolved address is blocked", async () => {
    await expect(
      assertSafeTarget("mixed.example", false, () =>
        Promise.resolve(["93.184.216.34", "10.0.0.1"]),
      ),
    ).rejects.toMatchObject({ code: "SSRF_BLOCKED" });
    await expect(
      assertSafeTarget("mixed.example", true, () => Promise.resolve(["93.184.216.34", "10.0.0.1"])),
    ).resolves.toBeUndefined();
  });

  it("accepts all-public answers and surfaces resolver failures", async () => {
    await expect(
      assertSafeTarget("public.example", false, () => Promise.resolve(["93.184.216.34"])),
    ).resolves.toBeUndefined();
    await expect(
      assertSafeTarget("missing.example", false, () => Promise.reject(new Error("ENOTFOUND"))),
    ).rejects.toMatchObject({ name: "ExecutorError" });
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

describe("log redaction", () => {
  it("never writes header values, bodies, or credentials to logs", async () => {
    const { Writable } = await import("node:stream");
    const { createLogger } = await import("../../src/infrastructure/logging/logger.js");
    const lines: string[] = [];
    const sink = new Writable({
      write(chunk: unknown, _encoding, callback) {
        lines.push(String(chunk));
        callback();
      },
    });
    const logger = createLogger({ level: "debug", destination: sink });
    const secret = `log-secret-${Date.now()}`;
    const executor = new HttpExecutor(OPTIONS, logger);
    // Successful request: emits the debug request line while secrets are
    // in play (headers + JSON body).
    const result = await executor.execute({
      ...ctxFor(`${baseUrl}/echo`),
      execution: {
        type: "http",
        url: `${baseUrl}/echo`,
        method: "POST",
        headers: { authorization: `Bearer ${secret}`, "x-api-key": secret },
        body: { password: secret },
      },
    });
    expect(result.statusCode).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const output = lines.join("\n");
    expect(lines.length).toBeGreaterThan(0);
    expect(output).not.toContain(secret);
  });
});
