import type { Redis } from "ioredis";
import { ApiError, classifyBackendError } from "../api/errors.js";
import { generateConsumerId, generateMessageId } from "./ids.js";
import { escapeGlob, messageKey, pendingKey, queueKeys } from "./keys.js";
import {
  ACK_SCRIPT,
  CANCEL_CONSUMER_SCRIPT,
  CONSUME_SCRIPT,
  DECLARE_SCRIPT,
  DELETE_MESSAGE_SCRIPT,
  PUBLISH_SCRIPT,
  REQUEUE_SCRIPT,
  SET_TTL_SCRIPT,
  STATS_SCRIPT,
  SWEEP_SCRIPT,
} from "./lua.js";
import type {
  BrokerMessage,
  ConsumeResult,
  ConsumerInfo,
  Json,
  MessageState,
  QueueStats,
  QueueSummary,
} from "./types.js";

export interface BrokerOptions {
  prefix: string;
  defaultVisibilityTimeoutMs: number;
  defaultPrefetch: number;
  maxConsumeCount: number;
  maxMessageBytes: number;
}

export interface PublishOptions {
  id?: string | undefined;
  /** Delay before the message becomes available (ms from now). */
  ttlMs?: number | undefined;
}

export interface PublishedMessage {
  id: string;
  queue: string;
  state: MessageState;
  availableAt: number;
  createdAt: number;
}

export interface ConsumeOptions {
  consumerId?: string | undefined;
  count?: number | undefined;
  visibilityTimeoutMs?: number | undefined;
  /** Max leased messages per consumer (-1/undefined keeps the stored value). */
  prefetch?: number | undefined;
}

type ScriptCaller = (...args: Array<string | number>) => Promise<unknown>;

function isMessageState(value: unknown): value is MessageState {
  return value === "ready" || value === "delayed" || value === "unacked";
}

/** Flatten a Lua array reply into strings (nested tables stay nested). */
function asArray(reply: unknown, script: string): unknown[] {
  if (!Array.isArray(reply)) {
    throw ApiError.internal(`Unexpected reply from ${script}.`);
  }
  return reply;
}

function asString(value: unknown, script: string): string {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  throw ApiError.internal(`Unexpected reply from ${script}.`);
}

function asNumber(value: unknown, script: string): number {
  const num = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(num)) {
    throw ApiError.internal(`Unexpected reply from ${script}.`);
  }
  return num;
}

/** Parse a flat [field, value, ...] array into a record. */
function flatToRecord(value: unknown, script: string): Record<string, string> {
  const items = asArray(value, script);
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < items.length; i += 2) {
    record[asString(items[i], script)] = asString(items[i + 1], script);
  }
  return record;
}

function parseMessageHash(record: Record<string, string>, queue: string): BrokerMessage {
  const stateRaw = record["state"];
  if (!isMessageState(stateRaw)) {
    throw ApiError.internal("Unexpected message state in backend.");
  }
  let data: Json;
  try {
    data = JSON.parse(record["data"] ?? "null") as Json;
  } catch {
    throw ApiError.internal("Stored message payload is corrupt.");
  }
  const consumer = record["consumer"] ?? "";
  return {
    id: record["id"] ?? "",
    queue,
    data,
    state: stateRaw,
    consumerId: consumer === "" ? null : consumer,
    deliveryCount: Number(record["deliveries"] ?? "0"),
    availableAt: Number(record["availableAt"] ?? "0"),
    visibleAt: Number(record["visibleAt"] ?? "0"),
    createdAt: Number(record["createdAt"] ?? "0"),
    updatedAt: Number(record["updatedAt"] ?? "0"),
  };
}

/**
 * Redis-backed competing-consumer message broker.
 *
 * All multi-step mutations run inside Lua scripts, so any number of
 * consumers can share a queue safely. Queues are durable: every message
 * lives in Redis until it is acked or deleted.
 */
export class BrokerService {
  private readonly scripts = new Map<string, ScriptCaller>();

  constructor(
    private readonly redis: Redis,
    private readonly options: BrokerOptions,
  ) {
    this.register("easymqDeclare", DECLARE_SCRIPT, 2);
    this.register("easymqPublish", PUBLISH_SCRIPT, 5);
    this.register("easymqConsume", CONSUME_SCRIPT, 7);
    this.register("easymqAck", ACK_SCRIPT, 3);
    this.register("easymqRequeue", REQUEUE_SCRIPT, 4);
    this.register("easymqDeleteMessage", DELETE_MESSAGE_SCRIPT, 4);
    this.register("easymqSetTtl", SET_TTL_SCRIPT, 4);
    this.register("easymqCancelConsumer", CANCEL_CONSUMER_SCRIPT, 5);
    this.register("easymqSweep", SWEEP_SCRIPT, 4);
    this.register("easymqStats", STATS_SCRIPT, 6);
  }

  private register(name: string, lua: string, numberOfKeys: number): void {
    this.redis.defineCommand(name, { lua, numberOfKeys });
    const caller: ScriptCaller = (...args) => {
      const fn = (this.redis as unknown as Record<string, ScriptCaller | undefined>)[name];
      if (!fn) throw ApiError.internal(`Script ${name} is not registered.`);
      // ioredis custom commands read client state via `this` — keep it bound.
      return fn.apply(this.redis, args);
    };
    this.scripts.set(name, caller);
  }

  private call(name: string, args: Array<string | number>): Promise<unknown> {
    const caller = this.scripts.get(name);
    if (!caller) throw ApiError.internal(`Script ${name} is not registered.`);
    return caller(...args);
  }

  /** Idempotent queue declaration. */
  async declareQueue(queue: string): Promise<{ queue: string; created: boolean }> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const created = await this.call("easymqDeclare", [
        keys.registry,
        keys.meta,
        queue,
        Date.now(),
      ]);
      return { queue, created: asNumber(created, "declare") === 1 };
    } catch (err) {
      throw classifyBackendError(err, "declare queue");
    }
  }

  async listQueues(): Promise<QueueSummary[]> {
    try {
      const names = await this.redis.smembers(queueKeys(this.options.prefix, "").registry);
      const sorted = [...names].sort();
      if (sorted.length === 0) return [];
      const pipeline = this.redis.pipeline();
      for (const name of sorted) {
        const keys = queueKeys(this.options.prefix, name);
        pipeline.llen(keys.ready);
        pipeline.zcard(keys.delayed);
        pipeline.zcard(keys.unacked);
        pipeline.hlen(keys.consumers);
      }
      const results = await pipeline.exec();
      const summaries: QueueSummary[] = sorted.map((name, index) => {
        const base = index * 4;
        const num = (offset: number): number => {
          const entry = results?.[base + offset];
          const value = Array.isArray(entry) ? entry[1] : undefined;
          return typeof value === "number" ? value : 0;
        };
        return {
          queue: name,
          ready: num(0),
          delayed: num(1),
          unacked: num(2),
          consumers: num(3),
        };
      });
      return summaries;
    } catch (err) {
      throw classifyBackendError(err, "list queues");
    }
  }

  async getQueue(queue: string): Promise<QueueStats> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("easymqStats", [
        keys.registry,
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        keys.consumers,
        queue,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "inspect queue");
    }
    const parts = asArray(reply, "stats");
    if (asString(parts[0], "stats") === "NOT_FOUND") {
      throw ApiError.notFound("queue", queue);
    }
    const ready = asNumber(parts[1], "stats");
    const delayed = asNumber(parts[2], "stats");
    const unacked = asNumber(parts[3], "stats");
    const consumersFlat = asArray(parts[4], "stats");
    const meta = flatToRecord(parts[5], "stats");
    const ids: string[] = [];
    const prefetches = new Map<string, number>();
    for (let i = 0; i + 1 < consumersFlat.length; i += 2) {
      const id = asString(consumersFlat[i], "stats");
      const prefetch = asNumber(consumersFlat[i + 1], "stats");
      ids.push(id);
      prefetches.set(id, prefetch);
    }
    let consumers: ConsumerInfo[] = ids.map((id) => ({
      id,
      prefetch: prefetches.get(id) ?? this.options.defaultPrefetch,
      unacked: 0,
    }));
    try {
      if (ids.length > 0) {
        const pipeline = this.redis.pipeline();
        for (const id of ids) pipeline.scard(pendingKey(keys, id));
        const results = await pipeline.exec();
        consumers = ids.map((id, index) => {
          const entry = results?.[index];
          const value = Array.isArray(entry) ? entry[1] : undefined;
          return {
            id,
            prefetch: prefetches.get(id) ?? this.options.defaultPrefetch,
            unacked: typeof value === "number" ? value : 0,
          };
        });
      }
    } catch (err) {
      throw classifyBackendError(err, "inspect queue");
    }
    consumers.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return {
      queue,
      ready,
      delayed,
      unacked,
      consumers,
      published: Number(meta["published"] ?? "0"),
      delivered: Number(meta["delivered"] ?? "0"),
      acked: Number(meta["acked"] ?? "0"),
      requeued: Number(meta["requeued"] ?? "0"),
      deleted: Number(meta["deleted"] ?? "0"),
      createdAt: Number(meta["createdAt"] ?? "0"),
    };
  }

  /** Delete a queue and every message in it. In-flight acks afterwards 404. */
  async deleteQueue(queue: string): Promise<void> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const exists = await this.redis.sismember(keys.registry, queue);
      if (exists !== 1) throw ApiError.notFound("queue", queue);
      const consumerIds = await this.redis.hkeys(keys.consumers);
      const targets: string[] = [
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        keys.consumers,
        ...consumerIds.map((id) => pendingKey(keys, id)),
      ];
      const match = `${escapeGlob(keys.messagePrefix)}*`;
      let cursor = "0";
      do {
        const [next, found] = await this.redis.scan(cursor, "MATCH", match, "COUNT", 500);
        cursor = next;
        targets.push(...found);
      } while (cursor !== "0");
      const pipeline = this.redis.pipeline();
      for (const key of targets) pipeline.unlink(key);
      pipeline.srem(keys.registry, queue);
      await pipeline.exec();
    } catch (err) {
      throw classifyBackendError(err, "delete queue");
    }
  }

  async publish(queue: string, data: Json, opts: PublishOptions = {}): Promise<PublishedMessage> {
    const dataJson = JSON.stringify(data);
    if (Buffer.byteLength(dataJson, "utf8") > this.options.maxMessageBytes) {
      throw ApiError.validation(
        `Message payload exceeds the ${String(this.options.maxMessageBytes)} byte limit.`,
      );
    }
    const keys = queueKeys(this.options.prefix, queue);
    const ttlMs = opts.ttlMs ?? 0;
    const now = Date.now();
    const availableAt = now + ttlMs;
    const requestedId = opts.id;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const id = requestedId ?? generateMessageId();
      let reply: unknown;
      try {
        reply = await this.call("easymqPublish", [
          keys.registry,
          keys.meta,
          keys.ready,
          keys.delayed,
          messageKey(keys, id),
          queue,
          id,
          dataJson,
          availableAt,
          now,
        ]);
      } catch (err) {
        throw classifyBackendError(err, "publish message");
      }
      const parts = asArray(reply, "publish");
      const status = asString(parts[0], "publish");
      if (status === "CONFLICT") {
        if (requestedId !== undefined) {
          throw new ApiError("CONFLICT", `Message '${requestedId}' already exists.`, {
            resource: { type: "message", id: requestedId, queue },
          });
        }
        continue; // generated id collided — retry (practically impossible)
      }
      const state = asString(parts[1], "publish");
      if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from publish.");
      return { id, queue, state, availableAt, createdAt: now };
    }
    throw ApiError.internal("Failed to allocate a message id.");
  }

  async getMessage(queue: string, id: string): Promise<BrokerMessage> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const record = await this.redis.hgetall(messageKey(keys, id));
      if (Object.keys(record).length === 0) {
        throw ApiError.notFound("message", id, queue);
      }
      return parseMessageHash(record, queue);
    } catch (err) {
      throw classifyBackendError(err, "inspect message");
    }
  }

  async consume(queue: string, opts: ConsumeOptions = {}): Promise<ConsumeResult> {
    const keys = queueKeys(this.options.prefix, queue);
    const consumerId = opts.consumerId ?? generateConsumerId();
    const count = Math.min(Math.max(opts.count ?? 1, 1), this.options.maxConsumeCount);
    const visibilityMs = opts.visibilityTimeoutMs ?? this.options.defaultVisibilityTimeoutMs;
    const prefetch = opts.prefetch ?? -1;
    const now = Date.now();
    let reply: unknown;
    try {
      reply = await this.call("easymqConsume", [
        keys.registry,
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        keys.consumers,
        pendingKey(keys, consumerId),
        queue,
        consumerId,
        count,
        prefetch,
        visibilityMs,
        now,
        500,
        keys.messagePrefix,
        keys.pendingPrefix,
        this.options.defaultPrefetch,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "consume messages");
    }
    const parts = asArray(reply, "consume");
    if (asString(parts[0], "consume") === "NOT_FOUND") {
      throw ApiError.notFound("queue", queue);
    }
    const delivered = asNumber(parts[1], "consume");
    const messages = [];
    for (let i = 0; i < delivered; i += 1) {
      const id = asString(parts[3 + i * 3], "consume");
      const dataRaw = asString(parts[3 + i * 3 + 1], "consume");
      const deliveries = asNumber(parts[3 + i * 3 + 2], "consume");
      let data: Json;
      try {
        data = JSON.parse(dataRaw) as Json;
      } catch {
        throw ApiError.internal("Stored message payload is corrupt.");
      }
      messages.push({
        id,
        queue,
        data,
        deliveryCount: deliveries,
        redelivered: deliveries > 1,
        visibleAt: now + visibilityMs,
      });
    }
    return { consumerId, messages };
  }

  async ack(queue: string, id: string, consumerId?: string): Promise<{ deliveries: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("easymqAck", [
        keys.meta,
        keys.unacked,
        messageKey(keys, id),
        id,
        consumerId ?? "",
        Date.now(),
        keys.pendingPrefix,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "acknowledge message");
    }
    return { deliveries: this.settledLease(reply, "ack", queue, id, "ack") };
  }

  async requeue(queue: string, id: string, consumerId?: string): Promise<{ deliveries: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("easymqRequeue", [
        keys.meta,
        keys.ready,
        keys.unacked,
        messageKey(keys, id),
        id,
        consumerId ?? "",
        Date.now(),
        keys.pendingPrefix,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "requeue message");
    }
    return { deliveries: this.settledLease(reply, "requeue", queue, id, "requeue") };
  }

  private settledLease(
    reply: unknown,
    script: string,
    queue: string,
    id: string,
    verb: string,
  ): number {
    const parts = asArray(reply, script);
    const status = asString(parts[0], script);
    if (status === "OK") return asNumber(parts[1], script);
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "WRONG_OWNER") {
      throw new ApiError("CONFLICT", `Message '${id}' is leased to another consumer.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1] ?? "", script);
    throw new ApiError(
      "CONFLICT",
      `Cannot ${verb} message '${id}' while it is ${state === "" ? "unavailable" : state}.`,
      { resource: { type: "message", id, queue } },
    );
  }

  async deleteMessage(queue: string, id: string): Promise<{ state: MessageState }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("easymqDeleteMessage", [
        keys.meta,
        keys.ready,
        keys.delayed,
        messageKey(keys, id),
        id,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "delete message");
    }
    const parts = asArray(reply, "deleteMessage");
    const status = asString(parts[0], "deleteMessage");
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "CONFLICT") {
      throw new ApiError("CONFLICT", `Message '${id}' is unacked; ack or requeue it first.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1], "deleteMessage");
    if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from deleteMessage.");
    return { state };
  }

  async setMessageTtl(
    queue: string,
    id: string,
    ttlMs: number,
  ): Promise<{ state: MessageState; availableAt: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    const now = Date.now();
    const availableAt = now + ttlMs;
    let reply: unknown;
    try {
      reply = await this.call("easymqSetTtl", [
        keys.meta,
        keys.ready,
        keys.delayed,
        messageKey(keys, id),
        id,
        availableAt,
        now,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "change message TTL");
    }
    const parts = asArray(reply, "setTtl");
    const status = asString(parts[0], "setTtl");
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "CONFLICT") {
      throw new ApiError("CONFLICT", `Message '${id}' is unacked; ack or requeue it first.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1], "setTtl");
    if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from setTtl.");
    return { state, availableAt };
  }

  async cancelConsumer(queue: string, consumerId: string): Promise<{ requeued: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("easymqCancelConsumer", [
        keys.meta,
        keys.ready,
        keys.unacked,
        keys.consumers,
        pendingKey(keys, consumerId),
        consumerId,
        Date.now(),
        1000,
        keys.messagePrefix,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "cancel consumer");
    }
    const parts = asArray(reply, "cancelConsumer");
    return { requeued: asNumber(parts[1], "cancelConsumer") };
  }

  /** Promote due + reclaim expired for one queue (used by the sweeper). */
  async sweep(queue: string): Promise<{ promoted: number; reclaimed: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const reply = await this.call("easymqSweep", [
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        Date.now(),
        500,
        keys.pendingPrefix,
        keys.messagePrefix,
      ]);
      const parts = asArray(reply, "sweep");
      return {
        promoted: asNumber(parts[1], "sweep"),
        reclaimed: asNumber(parts[2], "sweep"),
      };
    } catch (err) {
      throw classifyBackendError(err, "sweep queue");
    }
  }

  async knownQueues(): Promise<string[]> {
    try {
      return await this.redis.smembers(queueKeys(this.options.prefix, "").registry);
    } catch (err) {
      throw classifyBackendError(err, "list queues");
    }
  }
}
