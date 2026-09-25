import { ApiError } from "../api/errors.js";
import type { Logger } from "../infrastructure/logging/logger.js";

export interface ReadinessCheck {
  name: string;
  check: () => Promise<void>;
}

export interface LivenessStatus {
  status: "ok";
  service: "easymq";
  uptimeSeconds: number;
}

export interface ReadinessStatus {
  status: "ok";
  service: "easymq";
  checks: Array<{ name: string; ok: boolean; error?: string }>;
}

/**
 * Health reporting. Liveness never touches Redis (a degraded Redis must
 * not kill the process); readiness verifies Redis reachability. Routes
 * contain no Redis logic themselves.
 */
export class HealthService {
  private readonly checks: ReadinessCheck[] = [];
  private readonly startedAt = Date.now();

  constructor(private readonly logger?: Logger) {}

  addCheck(check: ReadinessCheck): void {
    this.checks.push(check);
  }

  liveness(): LivenessStatus {
    return {
      status: "ok",
      service: "easymq",
      uptimeSeconds: Math.floor((Date.now() - this.startedAt) / 1000),
    };
  }

  async readiness(): Promise<ReadinessStatus> {
    const results = await Promise.all(
      this.checks.map(async ({ name, check }) => {
        try {
          await check();
          return { name, ok: true as const };
        } catch (err) {
          // Readiness is intentionally unauthenticated for orchestrators.
          // Keep backend details in server logs, never in the probe response.
          this.logger?.warn({ err, check: name }, "Readiness check failed");
          return {
            name,
            ok: false as const,
            error: "unavailable",
          };
        }
      }),
    );
    const failed = results.filter((r) => !r.ok);
    if (failed.length > 0) {
      throw new ApiError("SERVICE_UNAVAILABLE", "Service is not ready.", {
        details: { checks: results },
      });
    }
    return {
      status: "ok",
      service: "easymq",
      checks: results,
    };
  }
}
