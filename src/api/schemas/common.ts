import { z } from "zod";

export const queueNameSchema = z
  .string()
  .min(1, "Queue name must not be empty.")
  .max(200, "Queue name must be at most 200 characters.")
  // eslint-disable-next-line no-control-regex
  .refine((name) => !/[\u0000-\u001f\u007f]/.test(name), {
    message: "Queue name must not contain control characters.",
  });

export const jobIdSchema = z
  .string()
  .min(1, "Job id must not be empty.")
  .max(500, "Job id must be at most 500 characters.");

export const paginationQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(1000).optional(),
  offset: z.coerce.number().int().min(0).optional(),
  asc: z
    .union([z.boolean(), z.string()])
    .optional()
    .transform((value) => {
      if (value === undefined) return undefined;
      if (typeof value === "boolean") return value;
      const normalized = value.trim().toLowerCase();
      if (["1", "true", "yes"].includes(normalized)) return true;
      if (["0", "false", "no"].includes(normalized)) return false;
      return value;
    })
    .pipe(z.boolean().optional()),
});

export type PaginationQuery = z.infer<typeof paginationQuerySchema>;
