import { setTimeout as sleep } from "node:timers/promises";
import amqp from "amqplib";
import { config } from "./config.js";
import { parseTaskMessage } from "./message.js";

const MAX_RETRIES = 3;
const MAIN_EXCHANGE = "rabbitmq-demo.events";
const MAIN_QUEUE = "rabbitmq-demo.tasks";
const ROUTING_KEY = "task.created";
const RETRY_EXCHANGE = "rabbitmq-demo.retry";
const RETRY_QUEUE = "rabbitmq-demo.tasks.retry";
const DEAD_EXCHANGE = "rabbitmq-demo.dead";
const DEAD_QUEUE = "rabbitmq-demo.tasks.dead";

const connection = await amqp.connect(config.rabbitmqUrl);
const channel = await connection.createConfirmChannel();

await channel.assertExchange(MAIN_EXCHANGE, "topic", { durable: true });
await channel.assertExchange(RETRY_EXCHANGE, "direct", { durable: true });
await channel.assertExchange(DEAD_EXCHANGE, "direct", { durable: true });

await channel.assertQueue(MAIN_QUEUE, { durable: true });
await channel.bindQueue(MAIN_QUEUE, MAIN_EXCHANGE, ROUTING_KEY);

await channel.assertQueue(RETRY_QUEUE, {
  durable: true,
  arguments: {
    "x-message-ttl": 3_000,
    "x-dead-letter-exchange": MAIN_EXCHANGE,
    "x-dead-letter-routing-key": ROUTING_KEY,
  },
});
await channel.bindQueue(RETRY_QUEUE, RETRY_EXCHANGE, ROUTING_KEY);

await channel.assertQueue(DEAD_QUEUE, { durable: true });
await channel.bindQueue(DEAD_QUEUE, DEAD_EXCHANGE, ROUTING_KEY);

await channel.prefetch(config.prefetch);

await channel.consume(MAIN_QUEUE, async (delivery) => {
  if (delivery === null) return;
  const retryCount = Number(
    delivery.properties.headers?.["x-retry-count"] ?? 0,
  );
  try {
    const task = parseTaskMessage(delivery.content.toString());
    console.log("开始处理", { id: task.id, attempt: retryCount + 1, task });
    await sleep(config.processingDelayMs);
    if (task.payload.shouldFail) throw new Error("模拟业务失败");
    channel.ack(delivery);
    console.log("处理成功", { id: task.id });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    if (retryCount < MAX_RETRIES) {
      channel.publish(RETRY_EXCHANGE, ROUTING_KEY, delivery.content, {
        ...delivery.properties,
        persistent: true,
        headers: {
          ...delivery.properties.headers,
          "x-retry-count": retryCount + 1,
        },
      });
      await channel.waitForConfirms();
      channel.ack(delivery);
      console.log("进入延迟重试队列", { retryCount: retryCount + 1, reason });
    } else {
      channel.publish(DEAD_EXCHANGE, ROUTING_KEY, delivery.content, {
        ...delivery.properties,
        persistent: true,
        headers: { ...delivery.properties.headers, "x-final-error": reason },
      });
      await channel.waitForConfirms();
      channel.ack(delivery);
      console.log("进入死信队列", { retryCount, reason });
    }
  }
});

console.log(`Consumer 已就绪，prefetch：${config.prefetch}`);
async function shutdown(): Promise<void> {
  console.log("\n正在优雅退出……");
  await channel.close();
  await connection.close();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
