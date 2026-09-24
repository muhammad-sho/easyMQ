import { z } from "zod";
import { jobIdSchema, queueNameSchema } from "./common.js";

export const httpMethodSchema = z.enum([
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "HEAD",
  "OPTIONS",
]);

function urlSchema() {
  return z
    .string()
    .min(1, "URL must not be empty.")
    .max(8000, "URL must be at most 8000 characters.")
    .refine(
      (value) => {
        try {
          const url = new URL(value);
          return url.protocol === "http:" || url.protocol === "https:";
        } catch {
          return false;
        }
      },
      { message: "URL must be a valid absolute http(s) URL." },
    );
}

export const httpExecutionSchema = z
  .object({
    type: z.literal("http"),
    url: urlSchema(),
    method: httpMethodSchema.optional(),
    headers: z.record(z.string(), z.string()).optional(),
    body: z.union([z.string(), z.json()]).optional(),
    timeoutMs: z.number().int().min(100).max(600_000).optional(),
    allowPrivateNetwork: z.boolean().optional(),
  })
  .strict();

export const executionSchema = z.discriminatedUnion("type", [httpExecutionSchema]);

export const backoffSchema = z
  .object({
    type: z.enum(["fixed", "exponential"]),
    delayMs: z.number().int().min(0).max(3_600_000),
  })
  .strict();

export const deduplicationSchema = z
  .object({
    id: z.string().min(1).max(500),
    ttlMs: z.number().int().min(0).max(86_400_000).optional(),
    replace: z.boolean().optional(),
  })
  .strict();

export const debounceSchema = z
  .object({
    id: z.string().min(1).max(500),
    delayMs: z.number().int().min(0).max(86_400_000),
    replace: z.boolean().optional(),
  })
  .strict();

export const retentionSchema = z
  .object({
    count: z.number().int().min(0).max(1_000_000).optional(),
    ageSeconds: z.number().int().min(0).max(31_536_000).optional(),
  })
  .strict()
  .refine((value) => value.count !== undefined || value.ageSeconds !== undefined, {
    message: "Retention must set at least one of count or ageSeconds.",
  });

export const createJobSchema = z
  .object({
    queue: queueNameSchema,
    name: z.string().min(1).max(200).optional(),
    payload: z.json().optional(),
    execution: executionSchema,
    jobId: z.string().min(1).max(500).optional(),
    delayMs: z.number().int().min(0).max(31_536_000_000).optional(),
    attempts: z.number().int().min(1).max(100).optional(),
    backoff: backoffSchema.optional(),
    priority: z.number().int().min(0).max(2_097_151).optional(),
    lifo: z.boolean().optional(),
    deduplication: deduplicationSchema.optional(),
    debounce: debounceSchema.optional(),
    removeOnComplete: retentionSchema.optional(),
    removeOnFail: retentionSchema.optional(),
  })
  .strict()
  .refine((value) => !(value.deduplication && value.debounce), {
    message: "Only one of deduplication or debounce may be set.",
    path: ["deduplication"],
  });

export type CreateJobInput = z.infer<typeof createJobSchema>;

export const changeDelaySchema = z
  .object({
    delayMs: z.number().int().min(0).max(31_536_000_000),
  })
  .strict();

export type ChangeDelayInput = z.infer<typeof changeDelaySchema>;

export const queueParamsSchema = z.object({
  queue: queueNameSchema,
});

export const jobParamsSchema = z.object({
  queue: queueNameSchema,
  id: jobIdSchema,
});
