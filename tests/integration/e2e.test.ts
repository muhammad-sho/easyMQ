import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSystem, type BuiltSystem } from "../../src/app/build-app.js";
import { loadConfig } from "../../src/config/env.js";
import type { EasyMQJob } from "../../src/jobs/job-types.js";
import { REDIS_URL, ephemeralPort, uniquePrefix, waitFor } from "./helpers.js";

describe("end-to-end HTTP flow", () => {
  const prefix = uniquePrefix("e2e");
  let echo: Server;
  let echoUrl = "";
  let system: BuiltSystem;
  let apiBase = "";

  beforeAll(async () => {
    echo = createServer((req: IncomingMessage, res: ServerResponse) => {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      req.on("end", () => {
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ method: req.method, body }));
      });
    });
    await new Promise<void>((resolve) => echo.listen(0, "127.0.0.1", resolve));
    echoUrl = `http://127.0.0.1:${String(ephemeralPort(echo))}/hook`;

    const config = loadConfig({
      AUTH_DISABLED: "true",
      APP_ROLE: "both",
      REDIS_URL,
      REDIS_KEY_PREFIX: prefix,
      API_HOST: "127.0.0.1",
      API_PORT: "0",
      WORKER_CONCURRENCY: "5",
      HTTP_ALLOW_PRIVATE_NETWORK: "true",
      LOG_LEVEL: "silent",
    });
    system = await buildSystem(config);
    if (!system.fastifyApp) throw new Error("API role expected");
    await system.fastifyApp.listen({ host: "127.0.0.1", port: 0 });
    const address = system.fastifyApp.server.address() as AddressInfo;
    apiBase = `http://127.0.0.1:${String(address.port)}`;
    await system.workerManager?.start();
  });

  afterAll(async () => {
    await system?.close();
    await new Promise<void>((resolve, reject) =>
      echo.close((err) => (err ? reject(err) : resolve())),
    );
  });

  async function api(path: string, init?: RequestInit): Promise<{ status: number; body: unknown }> {
    const hasBody = init?.body !== undefined;
    const res = await fetch(`${apiBase}${path}`, {
      ...init,
      headers: {
        ...(hasBody ? { "content-type": "application/json" } : {}),
        ...(init?.headers ?? {}),
      },
    });
    const text = await res.text();
    return { status: res.status, body: text === "" ? null : (JSON.parse(text) as unknown) };
  }

  it("creates a job via HTTP and executes it against a real endpoint", async () => {
    const created = await api("/jobs", {
      method: "POST",
      body: JSON.stringify({
        queue: `q-${prefix}-e2e`,
        payload: { orderId: "123" },
        execution: { type: "http", url: echoUrl, method: "POST", body: { event: "ping" } },
      }),
    });
    expect(created.status).toBe(201);
    const job = created.body as EasyMQJob;
    expect(job.state).toBe("waiting");

    let latest: EasyMQJob = job;
    await waitFor(
      async () => {
        const res = await api(`/queues/${job.queue}/jobs/${job.id}`);
        latest = res.body as EasyMQJob;
        return latest.state === "completed";
      },
      { timeoutMs: 30_000, label: "e2e job to complete" },
    );
    const returnValue = latest.returnValue as { statusCode: number; body: string };
    expect(returnValue.statusCode).toBe(200);
    expect(returnValue.body).toContain("POST");
    expect(returnValue.body).toContain("ping");
  });

  it("exposes queues, counts, pause/resume and health over HTTP", async () => {
    const queue = `q-${prefix}-e2e`;
    const queues = await api("/queues");
    expect(queues.status).toBe(200);
    expect((queues.body as { queues: string[] }).queues).toContain(queue);

    const paused = await api(`/queues/${queue}/pause`, { method: "POST" });
    expect(paused.status).toBe(200);
    expect((paused.body as { isPaused: boolean }).isPaused).toBe(true);

    const counts = await api(`/queues/${queue}/counts`);
    expect(counts.status).toBe(200);

    const resumed = await api(`/queues/${queue}/resume`, { method: "POST" });
    expect((resumed.body as { isPaused: boolean }).isPaused).toBe(false);

    expect((await api("/health/live")).status).toBe(200);
    expect((await api("/health/ready")).status).toBe(200);
  });
});
