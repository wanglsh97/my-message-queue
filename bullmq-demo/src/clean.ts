import { createQueue, createRedis } from "./client.js";

const connection = createRedis();
const queue = createQueue(connection);
try {
  await queue.obliterate({ force: true });
  console.log("BullMQ 队列已清空");
} finally {
  await queue.close();
  await connection.quit();
}
