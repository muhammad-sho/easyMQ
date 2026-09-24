import { ApiError } from "../api/errors.js";
import type { AppRole } from "../config/schema.js";

export interface ReadinessCheck {
  name: string;
  check: () => Promise<void>;
}

export interface LivenessStatus {
  status: "ok";
  service: "easymq";
  role: AppRole;
  uptimeSeconds: number;
}

export interface ReadinessStatus {
  status: "ok";
  service: "easymq";
  role: AppRole;
  checks: Array<{ name: string; ok: boolean; error?: string }>;
}

/**
 * Health reporting. Liveness never touches Redis (a degraded Redis must
 * not kill the process); readiness verifies everything the configured
 * role needs. Routes contain no Redis logic themselves.
 */
export class HealthService {
  private readonly checks: ReadinessCheck[] = [];
  private readonly startedAt = Date.now();

  constructor(private readonly role: AppRole) {}

  addCheck(check: ReadinessCheck): void {
    this.checks.push(check);
  }

  liveness(): LivenessStatus {
    return {
      status: "ok",
      service: "easymq",
      role: this.role,
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
          return {
            name,
            ok: false as const,
            error: err instanceof Error ? err.message : String(err),
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
      role: this.role,
      checks: results,
    };
  }
}
