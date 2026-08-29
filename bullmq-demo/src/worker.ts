import { setTimeout as sleep } from "node:timers/promises";
import { Worker } from "bullmq";
import { config } from "./config.js";
import { createRedis, QUEUE_NAME } from "./client.js";

const connection = createRedis();
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    console.log("开始处理", { jobId: job.id, attempt: job.attemptsMade + 1, data: job.data });
    await sleep(config.processingDelayMs);
    if (job.data.payload.shouldFail) throw new Error("模拟业务失败");
    return { processedAt: new Date().toISOString() };
  },
  { connection, concurrency: config.concurrency },
);

worker.on("ready", () => console.log(`Worker 已就绪，并发数：${config.concurrency}`));
worker.on("completed", (job) => console.log("处理成功", { jobId: job.id }));
worker.on("failed", (job, error) =>
  console.log("处理失败或等待重试", { jobId: job?.id, attemptsMade: job?.attemptsMade, error: error.message }),
);

async function shutdown(): Promise<void> {
  console.log("正在优雅退出……");
  await worker.close();
  await connection.quit();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
