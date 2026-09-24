import type { JobSchedulerJson, RepeatOptions } from "bullmq";
import { ApiError } from "../api/errors.js";
import type { AppConfig } from "../config/schema.js";
import type { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";
import {
  type EasyMQSchedule,
  type Json,
  type ScheduleOptions,
  type StoredJobData,
} from "./job-types.js";
import { toBullMQJobOptions } from "./job-service.js";
import { assertValidQueueName, type QueueCatalog } from "../queues/queue-catalog.js";

export interface SchedulePage {
  schedules: EasyMQSchedule[];
  offset: number;
  limit: number;
  nextOffset: number | null;
}

function toEasyMQSchedule(queueName: string, scheduler: JobSchedulerJson): EasyMQSchedule {
  return {
    id: scheduler.id ?? scheduler.key,
    queue: queueName,
    name: scheduler.name,
    pattern: scheduler.pattern ?? null,
    everyMs: scheduler.every ?? null,
    timezone: scheduler.tz ?? null,
    nextRunAtMs: scheduler.next ?? null,
    iterationCount: scheduler.iterationCount ?? null,
    limit: scheduler.limit ?? null,
    startDateMs: scheduler.startDate ?? null,
    endDateMs: scheduler.endDate ?? null,
  };
}

function validateTimezone(timezone: string): void {
  try {
    if (!Intl.supportedValuesOf("timeZone").includes(timezone)) {
      throw new Error("unsupported");
    }
  } catch {
    throw ApiError.validation(`Unsupported timezone '${timezone}'.`, { timezone });
  }
}

/**
 * Recurring schedules backed by BullMQ's Job Scheduler
 * (`Queue.upsertJobScheduler`). easyMQ representation only —
 * BullMQ scheduler internals never leave this service.
 *
 * Timezones: `timezone` is passed to BullMQ and cron patterns are
 * evaluated in that zone (IANA name, e.g. "Europe/Berlin"). When omitted
 * the pattern is evaluated in UTC.
 */
export class ScheduleService {
  constructor(
    private readonly queues: QueueFactory,
    private readonly catalog: QueueCatalog,
    private readonly config: AppConfig,
  ) {}

  async upsertSchedule(opts: ScheduleOptions): Promise<EasyMQSchedule> {
    assertValidQueueName(opts.queue);
    if (opts.id.trim() === "") {
      throw ApiError.validation("Schedule id must be a non-empty string.");
    }
    const hasPattern = opts.pattern !== undefined;
    const hasEvery = opts.everyMs !== undefined;
    if (hasPattern === hasEvery) {
      throw ApiError.validation("Exactly one of pattern (cron) or everyMs (interval) must be set.");
    }
    if (hasEvery && (!Number.isFinite(opts.everyMs) || (opts.everyMs as number) <= 0)) {
      throw ApiError.validation("everyMs must be a positive number of milliseconds.");
    }
    if (opts.pattern !== undefined && opts.pattern.trim() === "") {
      throw ApiError.validation("pattern must be a non-empty cron expression.");
    }
    if (opts.timezone !== undefined) validateTimezone(opts.timezone);

    const data: StoredJobData = {
      version: 1,
      payload: opts.payload ?? null,
      execution: opts.execution,
    };

    // Template options exclude jobId/repeat/delay/deduplication (BullMQ type).
    const templateJobOptions = toBullMQJobOptions(
      {
        queue: opts.queue,
        payload: opts.payload ?? null,
        execution: opts.execution,
        ...(opts.name !== undefined ? { name: opts.name } : {}),
        ...(opts.attempts !== undefined ? { attempts: opts.attempts } : {}),
        ...(opts.backoff !== undefined ? { backoff: opts.backoff } : {}),
        ...(opts.priority !== undefined ? { priority: opts.priority } : {}),
        ...(opts.removeOnComplete !== undefined ? { removeOnComplete: opts.removeOnComplete } : {}),
        ...(opts.removeOnFail !== undefined ? { removeOnFail: opts.removeOnFail } : {}),
      },
      {
        attempts: this.config.defaultAttempts,
        backoffType: this.config.defaultBackoffType,
        backoffDelayMs: this.config.defaultBackoffDelayMs,
        removeOnCompleteCount: this.config.defaultRemoveOnCompleteCount,
        removeOnFailCount: this.config.defaultRemoveOnFailCount,
      },
    );
    const {
      jobId: _jobId,
      delay: _delay,
      deduplication: _dedup,
      ...templateOpts
    } = templateJobOptions;

    const queue = this.queues.getQueue(opts.queue);
    const repeatOpts: Omit<RepeatOptions, "key"> = {};
    if (opts.pattern !== undefined) repeatOpts.pattern = opts.pattern;
    if (opts.everyMs !== undefined) repeatOpts.every = opts.everyMs;
    if (opts.timezone !== undefined) repeatOpts.tz = opts.timezone;
    if (opts.startDateMs !== undefined) repeatOpts.startDate = opts.startDateMs;
    if (opts.endDateMs !== undefined) repeatOpts.endDate = opts.endDateMs;
    if (opts.limit !== undefined) repeatOpts.limit = opts.limit;
    try {
      await queue.upsertJobScheduler(opts.id, repeatOpts, {
        name: opts.name ?? opts.id,
        data,
        opts: templateOpts,
      });
    } catch (err) {
      throw ApiError.validation(
        `Invalid schedule: ${err instanceof Error ? err.message : String(err)}`,
        { cause: err instanceof Error ? err.message : String(err) },
      );
    }
    await this.catalog.register(opts.queue);
    return this.getSchedule(opts.queue, opts.id);
  }

  async getSchedule(queueName: string, id: string): Promise<EasyMQSchedule> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const scheduler = await this.queues.getQueue(queueName).getJobScheduler(id);
    if (!scheduler) throw ApiError.notFound("schedule", id, queueName);
    return toEasyMQSchedule(queueName, scheduler);
  }

  async listSchedules(queueName: string, offset = 0, limit?: number): Promise<SchedulePage> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const pageLimit = Math.min(
      Math.max(limit ?? this.config.pageDefaultLimit, 1),
      this.config.pageMaxLimit,
    );
    const start = Math.max(offset, 0);
    const queue = this.queues.getQueue(queueName);
    const [schedulers, total] = await Promise.all([
      queue.getJobSchedulers(start, start + pageLimit - 1, true),
      queue.getJobSchedulersCount(),
    ]);
    const schedules = schedulers.map((s) => toEasyMQSchedule(queueName, s));
    void total;
    return {
      schedules,
      offset: start,
      limit: pageLimit,
      nextOffset: schedules.length === pageLimit ? start + pageLimit : null,
    };
  }

  async removeSchedule(queueName: string, id: string): Promise<void> {
    assertValidQueueName(queueName);
    await this.ensureRegistered(queueName);
    const removed = await this.queues.getQueue(queueName).removeJobScheduler(id);
    if (!removed) throw ApiError.notFound("schedule", id, queueName);
  }

  private async ensureRegistered(name: string): Promise<void> {
    const registered = await this.catalog.list();
    if (!registered.includes(name)) {
      throw ApiError.notFound("queue", name);
    }
  }
}

export type { Json };
