import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { EasyMQErrorCode, HttpExecution, Json } from "../jobs/job-types.js";
import {
  ExecutionAbortedError,
  type ExecutionResult,
  type Executor,
  type JobExecutionContext,
} from "./executor.js";

/** Stable error raised for executor failures (mapped by the processor). */
export class ExecutorError extends Error {
  readonly code: EasyMQErrorCode;
  constructor(code: EasyMQErrorCode, message: string, options: { cause?: unknown } = {}) {
    super(message, options.cause !== undefined ? { cause: options.cause } : {});
    this.name = "ExecutorError";
    this.code = code;
  }
}

export interface HttpExecutorOptions {
  timeoutMs: number;
  maxResponseBytes: number;
  maxRedirects: number;
  allowPrivateNetwork: boolean;
}

/** Headers that must never be forwarded (hop-by-hop per RFC 9110). */
const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function ipv4ToInt(ip: string): number {
  const parts = ip.split(".").map(Number);
  return (
    ((parts[0] as number) * 256 ** 3 +
      (parts[1] as number) * 256 ** 2 +
      (parts[2] as number) * 256 +
      (parts[3] as number)) >>>
    0
  );
}

function inCidr(ip: string, cidr: string): boolean {
  const [base, bitsStr] = cidr.split("/");
  const bits = Number(bitsStr);
  if (isIP(ip) !== 4 || !base || !Number.isFinite(bits)) return false;
  if (bits === 0) return true;
  const mask = bits === 32 ? 0xffffffff : ~((1 << (32 - bits)) - 1) >>> 0;
  return (ipv4ToInt(ip) & mask) === (ipv4ToInt(base) & mask);
}

function ipv6ToBigInt(ip: string): bigint | null {
  // Expand :: shorthand, then fold 8 hextets.
  const halves = ip.split("::");
  if (halves.length > 2) return null;
  const head = (halves[0] ?? "").split(":").filter((p) => p !== "");
  const tail = halves.length === 2 ? (halves[1] ?? "").split(":").filter((p) => p !== "") : [];
  // Handle embedded IPv4 (e.g. ::ffff:1.2.3.4).
  const expandPart = (parts: string[]): string[] => {
    const out: string[] = [];
    for (const part of parts) {
      if (part.includes(".")) {
        const n = ipv4ToInt(part);
        out.push(((n >>> 16) & 0xffff).toString(16), (n & 0xffff).toString(16));
      } else {
        out.push(part);
      }
    }
    return out;
  };
  const headParts = expandPart(head);
  const tailParts = expandPart(tail);
  const missing = 8 - headParts.length - tailParts.length;
  if (missing < 0) return null;
  const full = [...headParts, ...Array<string>(missing).fill("0"), ...tailParts];
  try {
    return full.reduce((acc, h) => (acc << 16n) + BigInt(parseInt(h, 16)), 0n);
  } catch {
    return null;
  }
}

function ipv6InCidr(ip: string, cidr: string): boolean {
  const slash = cidr.indexOf("/");
  const base = cidr.slice(0, slash);
  const bits = Number(cidr.slice(slash + 1));
  const addr = ipv6ToBigInt(ip);
  const baseAddr = ipv6ToBigInt(base);
  if (addr === null || baseAddr === null || !Number.isFinite(bits)) return false;
  if (bits === 0) return true;
  const shift = 128n - BigInt(bits);
  return addr >> shift === baseAddr >> shift;
}

/** IPv4-mapped IPv6 (::ffff:a.b.c.d) unwraps to its IPv4 address. */
function unwrapIp(ip: string): { family: 4 | 6; normalized: string } {
  if (isIP(ip) === 6) {
    const lower = ip.toLowerCase();
    const marker = "::ffff:";
    const idx = lower.lastIndexOf(marker);
    if (idx !== -1) {
      const tail = ip.slice(idx + marker.length);
      if (isIP(tail) === 4) return { family: 4, normalized: tail };
    }
    return { family: 6, normalized: ip };
  }
  return { family: 4, normalized: ip };
}

const IPV4_ALWAYS_BLOCKED: Array<{ cidr: string; reason: string }> = [
  { cidr: "0.0.0.0/8", reason: "unspecified" },
  { cidr: "224.0.0.0/4", reason: "multicast" },
  { cidr: "240.0.0.0/4", reason: "reserved" },
  { cidr: "100.64.0.0/10", reason: "carrier-grade NAT" },
  { cidr: "192.0.2.0/24", reason: "documentation/reserved" },
  { cidr: "198.51.100.0/24", reason: "documentation/reserved" },
  { cidr: "203.0.113.0/24", reason: "documentation/reserved" },
  { cidr: "192.18.0.0/15", reason: "benchmark/reserved" },
];

const IPV4_PRIVATE_ONLY: Array<{ cidr: string; reason: string }> = [
  { cidr: "127.0.0.0/8", reason: "loopback" },
  { cidr: "169.254.0.0/16", reason: "link-local (includes cloud metadata services)" },
  { cidr: "10.0.0.0/8", reason: "private network" },
  { cidr: "172.16.0.0/12", reason: "private network" },
  { cidr: "192.168.0.0/16", reason: "private network" },
];

const IPV6_ALWAYS_BLOCKED: Array<{ cidr: string; reason: string }> = [
  { cidr: "::/128", reason: "unspecified" },
  { cidr: "ff00::/8", reason: "multicast" },
  { cidr: "2001:db8::/32", reason: "documentation/reserved" },
];

const IPV6_PRIVATE_ONLY: Array<{ cidr: string; reason: string }> = [
  { cidr: "::1/128", reason: "loopback" },
  { cidr: "fe80::/10", reason: "link-local" },
  { cidr: "fc00::/7", reason: "private network" },
];

/**
 * Cloud metadata-service IPs. Blocked unconditionally — even when private
 * network access is enabled — since they hand out instance credentials.
 */
const METADATA_IPS = new Set([
  "169.254.169.254", // AWS/GCP/Azure/OCI link-local metadata
  "100.100.100.100", // Alibaba Cloud metadata
  "fd00:ec2::254", // AWS IPv6 metadata
]);

/**
 * SSRF guard: resolve the hostname and reject unsafe destinations.
 * Private ranges are allowed only when explicitly configured
 * (self-hosted installations often call internal services).
 *
 * Note: resolution happens before connect (TOCTOU with DNS rebinding
 * is inherent to this approach and documented in the README).
 */
export async function assertSafeTarget(
  hostname: string,
  allowPrivateNetwork: boolean,
): Promise<void> {
  let addresses;
  try {
    addresses = await lookup(hostname, { all: true });
  } catch (err) {
    throw new ExecutorError("EXECUTOR_ERROR", `DNS lookup failed for '${hostname}'.`, {
      cause: err,
    });
  }
  for (const addr of addresses) {
    const { family, normalized } = unwrapIp(addr.address);
    if (METADATA_IPS.has(normalized.toLowerCase())) {
      throw new ExecutorError(
        "SSRF_BLOCKED",
        `Blocked request to cloud metadata-service address '${normalized}'.`,
      );
    }
    if (family === 4) {
      for (const { cidr, reason } of IPV4_ALWAYS_BLOCKED) {
        if (inCidr(normalized, cidr)) {
          throw new ExecutorError(
            "SSRF_BLOCKED",
            `Blocked request to ${reason} address '${normalized}'.`,
          );
        }
      }
      if (!allowPrivateNetwork) {
        for (const { cidr, reason } of IPV4_PRIVATE_ONLY) {
          if (inCidr(normalized, cidr)) {
            throw new ExecutorError(
              "SSRF_BLOCKED",
              `Blocked request to ${reason} address '${normalized}'. ` +
                `Enable HTTP_ALLOW_PRIVATE_NETWORK to call internal services.`,
            );
          }
        }
      }
    } else {
      for (const { cidr, reason } of IPV6_ALWAYS_BLOCKED) {
        if (ipv6InCidr(normalized, cidr)) {
          throw new ExecutorError(
            "SSRF_BLOCKED",
            `Blocked request to ${reason} address '${normalized}'.`,
          );
        }
      }
      if (!allowPrivateNetwork) {
        for (const { cidr, reason } of IPV6_PRIVATE_ONLY) {
          if (ipv6InCidr(normalized, cidr)) {
            throw new ExecutorError(
              "SSRF_BLOCKED",
              `Blocked request to ${reason} address '${normalized}'. ` +
                `Enable HTTP_ALLOW_PRIVATE_NETWORK to call internal services.`,
            );
          }
        }
      }
    }
  }
}

/** URL with any userinfo replaced — safe for logs. */
export function redactUrlForLogging(raw: string): string {
  try {
    const url = new URL(raw);
    if (url.username !== "" || url.password !== "") {
      url.username = "***";
      url.password = "";
    }
    return url.toString();
  } catch {
    return "[unparseable-url]";
  }
}

function combineSignals(signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }
    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      once: true,
    });
  }
  return controller.signal;
}

function bodyToString(body: Json | string | undefined): string | undefined {
  if (body === undefined) return undefined;
  return typeof body === "string" ? body : JSON.stringify(body);
}

/**
 * Generic outbound HTTP executor with redirect limits, response-size
 * limits, hop-by-hop header rejection, cross-origin credential stripping
 * and SSRF protection.
 */
export class HttpExecutor implements Executor {
  readonly type = "http" as const;

  constructor(
    private readonly options: HttpExecutorOptions,
    private readonly logger?: Logger,
  ) {}

  async execute(ctx: JobExecutionContext): Promise<ExecutionResult> {
    const started = Date.now();
    const execution = ctx.execution;
    const executionType = (execution as unknown as { type?: unknown }).type;
    if (executionType !== "http") {
      throw new ExecutorError(
        "EXECUTOR_ERROR",
        `HttpExecutor cannot handle execution type '${String(executionType)}'.`,
      );
    }
    const http = execution;
    const timeoutMs = http.timeoutMs ?? this.options.timeoutMs;
    const allowPrivate = http.allowPrivateNetwork ?? this.options.allowPrivateNetwork;

    let url: URL;
    try {
      url = new URL(http.url);
    } catch {
      throw new ExecutorError("EXECUTOR_ERROR", `Invalid URL '${http.url}'.`);
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new ExecutorError(
        "EXECUTOR_ERROR",
        `Unsupported URL protocol '${url.protocol}' (only http/https allowed).`,
      );
    }

    const method = (http.method ?? "GET").toUpperCase();
    const headers = this.buildHeaders(http.headers);
    const body = ["GET", "HEAD"].includes(method) ? undefined : bodyToString(http.body);
    if (body !== undefined && !hasContentType(headers)) {
      headers["content-type"] = typeof http.body === "string" ? "text/plain" : "application/json";
    }

    const timeoutController = new AbortController();
    const timeout = setTimeout(() => {
      timeoutController.abort(new Error("timeout"));
    }, timeoutMs);
    const signal = combineSignals([ctx.signal, timeoutController.signal]);

    try {
      const result = await this.requestWithRedirects({
        url,
        method,
        headers,
        body,
        signal,
        allowPrivate,
        queue: ctx.queue,
        jobId: ctx.jobId,
      });
      return { ...result, durationMs: Date.now() - started };
    } catch (err) {
      if (signal.aborted && !ctx.signal.aborted) {
        throw new ExecutorError(
          "EXECUTOR_TIMEOUT",
          `HTTP request timed out after ${String(timeoutMs)}ms.`,
        );
      }
      if (ctx.signal.aborted) throw new ExecutionAbortedError("Execution aborted.");
      if (err instanceof ExecutionAbortedError) throw err;
      if (err instanceof ExecutorError) throw err;
      throw new ExecutorError(
        "EXECUTOR_ERROR",
        `HTTP request failed: ${err instanceof Error ? err.message : String(err)}`,
        { cause: err },
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  private buildHeaders(input: Record<string, string> | undefined): Record<string, string> {
    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries(input ?? {})) {
      const lower = name.toLowerCase();
      if (HOP_BY_HOP.has(lower)) {
        throw new ExecutorError("EXECUTOR_ERROR", `Hop-by-hop header '${name}' must not be set.`);
      }
      headers[lower] = value;
    }
    return headers;
  }

  private async requestWithRedirects(args: {
    url: URL;
    method: string;
    headers: Record<string, string>;
    body: string | undefined;
    signal: AbortSignal;
    allowPrivate: boolean;
    queue: string;
    jobId: string;
  }): Promise<Omit<ExecutionResult, "durationMs">> {
    let { url, method, body } = args;
    const { headers, signal, allowPrivate, queue, jobId } = args;

    for (let redirect = 0; redirect <= this.options.maxRedirects; redirect++) {
      await assertSafeTarget(url.hostname, allowPrivate);
      this.logger?.debug(
        { event: "http-request", queue, jobId, url: redactUrlForLogging(url.toString()), method },
        "Outbound HTTP request",
      );

      const response: Response = await fetch(url, {
        method,
        headers,
        ...(body !== undefined ? { body } : {}),
        redirect: "manual",
        signal,
      });

      if (isRedirect(response.status)) {
        if (redirect === this.options.maxRedirects) {
          throw new ExecutorError(
            "REDIRECT_LIMIT_EXCEEDED",
            `Redirect limit of ${String(this.options.maxRedirects)} exceeded.`,
          );
        }
        const location = response.headers.get("location");
        await response.body?.cancel().catch(() => undefined);
        if (!location) {
          throw new ExecutorError(
            "EXECUTOR_ERROR",
            `Redirect (${String(response.status)}) without Location header.`,
          );
        }
        const next = new URL(location, url);
        if (next.protocol !== "http:" && next.protocol !== "https:") {
          throw new ExecutorError(
            "EXECUTOR_ERROR",
            `Redirect to unsupported protocol '${next.protocol}'.`,
          );
        }
        if (next.origin !== url.origin) {
          // Never forward credentials across origins.
          delete headers["authorization"];
          delete headers["cookie"];
        }
        if (response.status === 303 && method !== "HEAD") {
          method = "GET";
          body = undefined;
        } else if ((response.status === 301 || response.status === 302) && method === "POST") {
          method = "GET";
          body = undefined;
        }
        url = next;
        continue;
      }

      const responseHeaders: Record<string, string> = {};
      response.headers.forEach((value, key) => {
        responseHeaders[key] = value;
      });
      const { text, truncated } = await readBodyWithLimit(
        response,
        this.options.maxResponseBytes,
        signal,
      );
      return {
        statusCode: response.status,
        headers: responseHeaders,
        body: text,
        bodyTruncated: truncated,
      };
    }
    throw new ExecutorError(
      "REDIRECT_LIMIT_EXCEEDED",
      `Redirect limit of ${String(this.options.maxRedirects)} exceeded.`,
    );
  }
}

function isRedirect(status: number): boolean {
  return status === 301 || status === 302 || status === 303 || status === 307 || status === 308;
}

function hasContentType(headers: Record<string, string>): boolean {
  return Object.keys(headers).some((k) => k.toLowerCase() === "content-type");
}

async function readBodyWithLimit(
  response: Response,
  maxBytes: number,
  signal: AbortSignal,
): Promise<{ text: string | null; truncated: boolean }> {
  if (!response.body) {
    const text = await response.text();
    const bytes = Buffer.byteLength(text);
    if (bytes > maxBytes) {
      throw new ExecutorError(
        "RESPONSE_TOO_LARGE",
        `Response body of ${String(bytes)} bytes exceeds the limit of ${String(maxBytes)} bytes.`,
      );
    }
    return { text, truncated: false };
  }
  const reader = response.body.getReader() as ReadableStreamDefaultReader<Uint8Array>;
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      if (signal.aborted) {
        await reader.cancel().catch(() => undefined);
        throw new ExecutionAbortedError("Execution aborted.");
      }
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new ExecutorError(
          "RESPONSE_TOO_LARGE",
          `Response body exceeds the limit of ${String(maxBytes)} bytes.`,
        );
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  if (chunks.length === 0) return { text: "", truncated: false };
  const combined = Buffer.concat(chunks.map((c) => Buffer.from(c)));
  return { text: combined.toString("utf-8"), truncated: false };
}

export type { HttpExecution };
