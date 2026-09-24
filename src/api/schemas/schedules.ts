import { z } from "zod";
import { backoffSchema, executionSchema, retentionSchema } from "./jobs.js";
import { jobIdSchema, queueNameSchema } from "./common.js";

export const upsertScheduleSchema = z
  .object({
    id: jobIdSchema,
    queue: queueNameSchema,
    name: z.string().min(1).max(200).optional(),
    pattern: z.string().min(1).max(500).optional(),
    everyMs: z.number().int().min(1000).max(31_536_000_000).optional(),
    timezone: z.string().min(1).max(100).optional(),
    startDateMs: z.number().int().min(0).optional(),
    endDateMs: z.number().int().min(0).optional(),
    limit: z.number().int().min(1).max(1_000_000).optional(),
    payload: z.json().optional(),
    execution: executionSchema,
    attempts: z.number().int().min(1).max(100).optional(),
    backoff: backoffSchema.optional(),
    priority: z.number().int().min(0).max(2_097_151).optional(),
    removeOnComplete: retentionSchema.optional(),
    removeOnFail: retentionSchema.optional(),
  })
  .strict()
  .refine((value) => (value.pattern !== undefined) !== (value.everyMs !== undefined), {
    message: "Exactly one of pattern (cron) or everyMs (interval) must be set.",
    path: ["pattern"],
  });

export type UpsertScheduleInput = z.infer<typeof upsertScheduleSchema>;

export const scheduleParamsSchema = z.object({
  queue: queueNameSchema,
  id: jobIdSchema,
});
