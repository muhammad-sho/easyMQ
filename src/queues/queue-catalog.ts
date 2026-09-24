import type { Redis } from "ioredis";
import type { Logger } from "../infrastructure/logging/logger.js";
import { ApiError } from "../api/errors.js";

function assertValidQueueName(name: string): void {
  if (typeof name !== "string" || name.trim() === "" || name.length > 200) {
    throw ApiError.validation(
      "Queue name must be a non-empty string of at most 200 characters.",
      { queue: name },
    );
  }
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(name)) {
    throw ApiError.validation("Queue name must not contain control characters.", {
      queue: name,
    });
  }
}

export interface QueueCatalogOptions {
  /** Redis key prefix for easyMQ's own keys (BullMQ uses its own prefix). */
  keyPrefix: string;
}

type QueueRegisteredHandler = (queueName: string) => void;

/**
 * easyMQ queue discovery index.
 *
 * Holds queue names ONLY — never job state, payloads, retries or stats.
 * BullMQ remains the source of truth for all queue/job state.
 *
 * Registration is idempotent: SADD + PUBLISH happen atomically in a MULTI
 * so workers never miss a queue they should discover. There is no queue
 * deletion in v1 — queues stay registered even when empty.
 */
export class QueueCatalog {
  private readonly queuesKey: string;
  private readonly channel: string;
  private readonly handlers = new Set<QueueRegisteredHandler>();
  private readonly known = new Set<string>();
  private started = false;
  private subscriber: Redis | undefined;
  private onMessage: ((channel: string, message: string) => void) | undefined;

  constructor(
    private readonly redis: Redis,
    private readonly createSubscriber: () => Redis,
    options: QueueCatalogOptions,
    private readonly logger?: Logger,
  ) {
    const prefix = options.keyPrefix;
    this.queuesKey = `${prefix}:queues`;
    this.channel = `${prefix}:queue-registered`;
  }

  /** Idempotent registration. Returns true when the queue is newly added. */
  async register(queueName: string): Promise<boolean> {
    assertValidQueueName(queueName);
    const results = await this.redis
      .multi()
      .sadd(this.queuesKey, queueName)
      .publish(this.channel, queueName)
      .exec();
    if (!results) {
      throw ApiError.serviceUnavailable("Queue registration failed (no Redis response).");
    }
    const added = results[0]?.[1] as number;
    this.known.add(queueName);
    return added === 1;
  }

  async list(): Promise<string[]> {
    const names = await this.redis.smembers(this.queuesKey);
    return [...names].sort();
  }

  onQueueRegistered(handler: QueueRegisteredHandler): void {
    this.handlers.add(handler);
  }

  offQueueRegistered(handler: QueueRegisteredHandler): void {
    this.handlers.delete(handler);
  }

  /**
   * Subscribe to registration notifications and load the existing registry.
   * On reconnect, the known-set is reconciled so missed notifications
   * cannot prevent worker discovery (no polling involved).
   */
  async start(): Promise<void> {
    if (this.started) return;
    const subscriber = this.createSubscriber();
    this.subscriber = subscriber;
    this.onMessage = (_channel: string, message: string) => {
      this.emit(message);
    };
    subscriber.on("message", this.onMessage);
    // Reconcile after every (re)connect — catches missed notifications.
    subscriber.on("ready", () => {
      void this.reconcile().catch((err: unknown) => {
        this.logger?.error({ err }, "Queue registry reconcile failed");
      });
    });
    await subscriber.subscribe(this.channel);
    await this.reconcile();
    this.started = true;
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    const subscriber = this.subscriber;
    this.subscriber = undefined;
    if (this.onMessage && subscriber) {
      subscriber.off("message", this.onMessage);
      this.onMessage = undefined;
    }
    subscriber?.removeAllListeners("ready");
    try {
      await subscriber?.unsubscribe(this.channel);
    } catch {
      // ignore — shutting down
    }
  }

  private emit(queueName: string): void {
    const isNew = !this.known.has(queueName);
    this.known.add(queueName);
    if (!isNew) return; // idempotent: already-known queues are ignored
    for (const handler of this.handlers) {
      try {
        handler(queueName);
      } catch (err) {
        this.logger?.error({ err, queue: queueName }, "Queue registered handler failed");
      }
    }
  }

  private async reconcile(): Promise<void> {
    const names = await this.list();
    for (const name of names) {
      this.emit(name);
    }
  }
}

export { assertValidQueueName };
