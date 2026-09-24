import type { z } from "zod";
import { ApiError } from "../errors.js";

/** Parse unknown input with a Zod schema; throw 400 VALIDATION_ERROR. */
export function parseWith<T>(schema: z.ZodType<T>, input: unknown, what: string): T {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw ApiError.validation(`Invalid ${what}.`, {
      issues: result.error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
    });
  }
  return result.data;
}

/**
 * Normalize a query param that may be a single value, a repeated param
 * (array) or a comma-separated list into an array of trimmed strings.
 */
export function normalizeListParam(value: unknown): string[] {
  const values = Array.isArray(value) ? value : [value];
  const out: string[] = [];
  for (const item of values) {
    if (typeof item !== "string") continue;
    for (const part of item.split(",")) {
      const trimmed = part.trim();
      if (trimmed !== "") out.push(trimmed);
    }
  }
  return out;
}
