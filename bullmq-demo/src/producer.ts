import { config } from "./config.js";
import { createQueue, createRedis } from "./client.js";
import { createTaskMessage } from "./message.js";

const connection = createRedis();
const queue = createQueue(connection);

try {
  for (let sequence = 1; sequence <= config.messageCount; sequence += 1) {
    const message = createTaskMessage(sequence, config.demoFailures && sequence === 3);
    const job = await queue.add(message.type, message, {
      jobId: message.id,
      attempts: 3,
      backoff: { type: "exponential", delay: 1_000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    });
    console.log("任务已入队", { jobId: job.id, sequence });
  }
} finally {
  await queue.close();
  await connection.quit();
}
