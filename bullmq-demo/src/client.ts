import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { config } from "./config.js";
import type { TaskMessage } from "./message.js";

export const QUEUE_NAME = "bullmq-demo.tasks";

export function createRedis(): Redis {
  return new Redis(config.redisUrl, { maxRetriesPerRequest: null });
}

export function createQueue(connection: Redis): Queue<TaskMessage> {
  return new Queue<TaskMessage>(QUEUE_NAME, { connection });
}
