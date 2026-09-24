import { Queue, type DefaultJobOptions } from "bullmq";
import type { Logger } from "../logging/logger.js";
import type { RedisConnectionManager } from "../redis/connection-manager.js";

export interface QueueFactoryOptions {
  /** BullMQ key prefix (e.g. "easymq"). */
  prefix?: string;
  defaultJobOptions?: DefaultJobOptions;
}

/**
 * Cache of BullMQ Queue instances, one per queue name.
 * Each Queue gets its own dedicated Redis client owned by the
 * RedisConnectionManager so shutdown ordering stays explicit.
 */
export class QueueFactory {
  private readonly queues = new Map<string, Queue>();

  constructor(
    private readonly connections: RedisConnectionManager,
    private readonly options: QueueFactoryOptions = {},
    private readonly logger?: Logger,
  ) {}

  getQueue(name: string): Queue {
    const existing = this.queues.get(name);
    if (existing) return existing;
    const client = this.connections.createDedicated(`queue:${name}`);
    const queue = new Queue(name, {
      connection: client,
      ...(this.options.prefix !== undefined ? { prefix: this.options.prefix } : {}),
      ...(this.options.defaultJobOptions !== undefined
        ? { defaultJobOptions: this.options.defaultJobOptions }
        : {}),
    });
    queue.on("error", (err: Error) => {
      this.logger?.error({ err, queue: name }, "BullMQ Queue error");
    });
    this.queues.set(name, queue);
    return queue;
  }

  names(): string[] {
    return [...this.queues.keys()];
  }

  /** Close all Queue instances (their clients are closed via the manager). */
  async closeAll(): Promise<void> {
    const entries = [...this.queues.entries()];
    this.queues.clear();
    await Promise.all(
      entries.map(async ([name, queue]) => {
        try {
          await queue.close();
        } catch (err) {
          this.logger?.warn({ err, queue: name }, "Error closing queue");
        }
      }),
    );
  }
}
