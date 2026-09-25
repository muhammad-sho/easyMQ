import { z } from "zod";
import { consumerIdSchema, messageIdSchema } from "./common.js";

/** Publish body: a message is simply an id plus arbitrary JSON data. */
export const publishMessageSchema = z
  .object({
    id: messageIdSchema.optional(),
    data: z.json(),
    /** Delay before the message becomes available (ms from now). */
    ttlMs: z.number().int().min(0).max(2_592_000_000).optional(),
  })
  .strict();

/** Consume body: competing-consumer poll with lease + prefetch control. */
export const consumeSchema = z
  .object({
    consumerId: consumerIdSchema.optional(),
    /** Max messages to return in this call. */
    count: z.number().int().min(1).max(1000).optional(),
    /** Lease per delivered message: unacked past this it is redelivered. */
    visibilityTimeoutMs: z.number().int().min(100).max(43_200_000).optional(),
    /** Max messages leased to this consumer at once. */
    prefetch: z.number().int().min(1).max(1000).optional(),
  })
  .strict();

/** Optional owner check for ack/requeue (prevents acking another consumer's lease). */
export const leaseBodySchema = z
  .object({
    consumerId: consumerIdSchema.optional(),
  })
  .strict();

/** Change/reset a waiting message's TTL (ms from now; 0 = immediately available). */
export const setTtlSchema = z
  .object({
    ttl: z.number().int().min(0).max(2_592_000_000),
  })
  .strict();
