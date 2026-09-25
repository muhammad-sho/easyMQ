import { generateConsumerId } from "./ids.js";
import { ApiError } from "../api/errors.js";
import type { BrokerService } from "./broker.js";
import type { ConsumedMessage } from "./types.js";
import type { Logger } from "../infrastructure/logging/logger.js";

/** A message pushed to a persistent consumer, with routing metadata. */
export interface OutgoingMessage extends ConsumedMessage {
  queue: string;
  consumerId: string;
}

export interface SubscriberOptions {
  queue: string;
  /** Omitted consumerId is generated, exactly like the HTTP consume path. */
  consumerId?: string | undefined;
  /** Max messages leased to this consumer at once (server-enforced). */
  prefetch: number;
  visibilityTimeoutMs: number;
  /** Called once per delivered message; throwing marks the subscriber broken. */
  send: (message: OutgoingMessage) => void;
}

export interface SubscriberHandle {
  queue: string;
  consumerId: string;
}

interface Subscriber extends SubscriberHandle {
  prefetch: number;
  visibilityTimeoutMs: number;
  send: (message: OutgoingMessage) => void;
}

/**
 * Push delivery for persistent consumers (WebSocket subscriptions).
 *
 * Flow control stays entirely server-side: every fill is one atomic
 * `consume` call capped by the stored per-consumer prefetch, so
 * concurrent fills (change events, ack top-ups, reconnects) race
 * safely inside Lua with no local lease bookkeeping to drift.
 */
export class SubscriptionManager {
  private readonly subscribers = new Map<string, Map<string, Subscriber>>();
  private unsubscribeChange: (() => void) | undefined;

  constructor(
    private readonly broker: BrokerService,
    private readonly logger?: Logger,
  ) {}

  /** Start reacting to broker availability changes. */
  attach(): void {
    if (this.unsubscribeChange) return;
    this.unsubscribeChange = this.broker.onChange((queue) => {
      void this.fillQueue(queue).catch((err: unknown) => {
        this.logger?.warn({ err, queue, event: "fill-failed" }, "Subscription fill failed");
      });
    });
  }

  detach(): void {
    this.unsubscribeChange?.();
    this.unsubscribeChange = undefined;
  }

  subscriberCount(): number {
    let total = 0;
    for (const byConsumer of this.subscribers.values()) total += byConsumer.size;
    return total;
  }

  /**
   * Register a consumer and immediately deliver whatever is available.
   * Re-registering the same consumer id replaces the previous sender
   * (reconnect); the old socket stops receiving and its close handler
   * must not remove the replacement — use the returned handle.
   */
  async add(options: SubscriberOptions): Promise<SubscriberHandle> {
    const consumerId =
      options.consumerId !== undefined && options.consumerId !== ""
        ? options.consumerId
        : generateConsumerId();
    const subscriber: Subscriber = {
      queue: options.queue,
      consumerId,
      prefetch: options.prefetch,
      visibilityTimeoutMs: options.visibilityTimeoutMs,
      send: options.send,
    };
    let byConsumer = this.subscribers.get(options.queue);
    if (!byConsumer) {
      byConsumer = new Map();
      this.subscribers.set(options.queue, byConsumer);
    }
    byConsumer.set(consumerId, subscriber);
    await this.fillSubscriber(subscriber);
    return { queue: options.queue, consumerId };
  }

  /** Unregister without touching leases (the route cancels the consumer). */
  remove(handle: SubscriberHandle): void {
    const byConsumer = this.subscribers.get(handle.queue);
    if (byConsumer?.get(handle.consumerId) !== undefined) {
      // Only the currently registered sender may remove itself; a stale
      // socket from a replaced connection must not drop the replacement.
      byConsumer.delete(handle.consumerId);
      if (byConsumer.size === 0) this.subscribers.delete(handle.queue);
    }
  }

  /** Deliver newly available messages to one consumer (ack/requeue top-up). */
  async fillConsumer(handle: SubscriberHandle): Promise<void> {
    const subscriber = this.subscribers.get(handle.queue)?.get(handle.consumerId);
    if (!subscriber) return;
    await this.fillSubscriber(subscriber);
  }

  /** Cancel every consumer (requeues their pending messages) and reset. */
  async shutdown(): Promise<void> {
    this.detach();
    const handles: SubscriberHandle[] = [];
    for (const byConsumer of this.subscribers.values()) {
      for (const subscriber of byConsumer.values()) {
        handles.push({ queue: subscriber.queue, consumerId: subscriber.consumerId });
      }
    }
    this.subscribers.clear();
    for (const handle of handles) {
      try {
        await this.broker.cancelConsumer(handle.queue, handle.consumerId);
      } catch {
        // best effort during shutdown
      }
    }
  }

  private async fillQueue(queue: string): Promise<void> {
    const byConsumer = this.subscribers.get(queue);
    if (!byConsumer) return;
    for (const subscriber of byConsumer.values()) {
      try {
        await this.fillSubscriber(subscriber);
      } catch (err) {
        this.logger?.warn(
          { err, queue, consumer: subscriber.consumerId, event: "fill-failed" },
          "Subscription fill failed",
        );
      }
    }
  }

  private async fillSubscriber(subscriber: Subscriber): Promise<void> {
    // One atomic consume per iteration, capped server-side by prefetch.
    // Loop while a full batch arrives: the HTTP count cap
    // (maxConsumeCount) may be smaller than the consumer prefetch.
    let deliveredTotal = 0;
    for (;;) {
      let result: { messages: ConsumedMessage[] };
      try {
        result = await this.broker.consume(subscriber.queue, {
          consumerId: subscriber.consumerId,
          count: subscriber.prefetch,
          prefetch: subscriber.prefetch,
          visibilityTimeoutMs: subscriber.visibilityTimeoutMs,
        });
      } catch (err) {
        if (err instanceof ApiError && err.code === "NOT_FOUND") {
          // Queue vanished mid-subscription; the route closes the socket.
          this.remove(subscriber);
        }
        throw err;
      }
      if (result.messages.length === 0) return;
      for (const message of result.messages) {
        subscriber.send({
          ...message,
          queue: subscriber.queue,
          consumerId: subscriber.consumerId,
        });
      }
      deliveredTotal += result.messages.length;
      if (result.messages.length < subscriber.prefetch || deliveredTotal >= subscriber.prefetch) {
        return;
      }
    }
  }
}
