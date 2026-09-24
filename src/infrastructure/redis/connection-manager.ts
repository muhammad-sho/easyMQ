import { Redis } from "ioredis";
import type { Logger } from "../logging/logger.js";

/**
 * Owns every Redis connection easyMQ creates directly.
 *
 * BullMQ objects (Queue/Worker/QueueEvents) manage their own connections
 * internally; this manager only tracks the client instances easyMQ itself
 * constructs (shared client + dedicated clients handed to BullMQ + the
 * catalog pub/sub clients) so shutdown can close them in order.
 *
 * All clients use `maxRetriesPerRequest: null` as required by BullMQ
 * (blocking connections throw otherwise) and `enableOfflineQueue: false`
 * so failures surface instead of silently queuing.
 */
export class RedisConnectionManager {
  private readonly clients = new Set<Redis>();
  private shared: Redis | undefined;

  constructor(
    private readonly url: string,
    private readonly logger?: Logger,
  ) {}

  /**
   * Minimal options object (intentionally NOT typed as ioredis'
   * RedisOptions: that type declares optional fields with explicit
   * `| undefined`, which is not assignable to the constructor overloads
   * under exactOptionalPropertyTypes).
   */
  private buildOptions(): { maxRetriesPerRequest: null; enableOfflineQueue: false } {
    return {
      maxRetriesPerRequest: null,
      enableOfflineQueue: false,
    };
  }

  /** Shared client for lightweight commands (catalog, markers, pings). */
  getShared(): Redis {
    if (!this.shared) {
      this.shared = new Redis(this.url, this.buildOptions());
      this.shared.on("error", (err: Error) => {
        this.logger?.error({ err, event: "redis-error" }, "Redis error");
      });
      this.clients.add(this.shared);
    }
    return this.shared;
  }

  /** Dedicated client (BullMQ Queue/Worker/QueueEvents, pub/sub). */
  createDedicated(label = "dedicated"): Redis {
    const client = new Redis(this.url, {
      ...this.buildOptions(),
      connectionName: `easymq:${label}`,
    });
    client.on("error", (err: Error) => {
      this.logger?.error(
        { err, event: "redis-error", connection: label },
        "Redis error",
      );
    });
    this.clients.add(client);
    return client;
  }

  forget(client: Redis): void {
    this.clients.delete(client);
    if (this.shared === client) {
      this.shared = undefined;
    }
  }

  async closeAll(): Promise<void> {
    const clients = [...this.clients];
    this.clients.clear();
    this.shared = undefined;
    await Promise.all(
      clients.map(async (client) => {
        try {
          if (client.status === "end") return;
          await client.quit();
        } catch {
          try {
            client.disconnect();
          } catch {
            // ignore — we are shutting down
          }
        }
      }),
    );
  }

  get size(): number {
    return this.clients.size;
  }
}
