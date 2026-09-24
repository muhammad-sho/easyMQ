import type { JobType } from "bullmq";
import { ApiError } from "../api/errors.js";
import type { QueueInfo } from "../jobs/job-types.js";
import { assertValidQueueName, type QueueCatalog } from "./queue-catalog.js";
import type { QueueFactory } from "../infrastructure/bullmq/queue-factory.js";

/**
 * Count types requested for queue inspection. 'paused' is not part of
 * BullMQ's JobType union but is supported by the getCounts Lua script
 * at runtime (verified in bullmq 6.3.8 scripts/getCounts-1.js).
 */
const COUNT_TYPES = [
  "waiting",
  "active",
  "completed",
  "failed",
  "delayed",
  "prioritized",
  "paused",
  "waiting-children",
] as unknown as JobType[];

/**
 * Queue inspection and control. Uses BullMQ native operations only —
 * no custom pause mechanism, no cached statistics.
 */
export class QueueService {
  constructor(
    private readonly queues: QueueFactory,
    private readonly catalog: QueueCatalog,
  ) {}

  /** Registered queue names (the discovery index, sorted). */
  async listQueues(): Promise<string[]> {
    return this.catalog.list();
  }

  async getQueue(name: string): Promise<QueueInfo> {
    assertValidQueueName(name);
    await this.ensureRegistered(name);
    const queue = this.queues.getQueue(name);
    const [isPaused, counts] = await Promise.all([
      queue.isPaused(),
      queue.getJobCounts(...COUNT_TYPES),
    ]);
    return { name, isPaused, counts: { ...counts } };
  }

  async pauseQueue(name: string): Promise<QueueInfo> {
    assertValidQueueName(name);
    await this.ensureRegistered(name);
    await this.queues.getQueue(name).pause();
    return this.getQueue(name);
  }

  async resumeQueue(name: string): Promise<QueueInfo> {
    assertValidQueueName(name);
    await this.ensureRegistered(name);
    await this.queues.getQueue(name).resume();
    return this.getQueue(name);
  }

  async getJobCounts(name: string): Promise<Record<string, number>> {
    assertValidQueueName(name);
    await this.ensureRegistered(name);
    const counts = await this.queues.getQueue(name).getJobCounts(...COUNT_TYPES);
    return { ...counts };
  }

  private async ensureRegistered(name: string): Promise<void> {
    const registered = await this.catalog.list();
    if (!registered.includes(name)) {
      throw ApiError.notFound("queue", name);
    }
  }
}
