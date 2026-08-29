import { setTimeout as sleep } from "node:timers/promises";
import amqp from "amqplib";
import { config } from "./config.js";
import { parseTaskMessage } from "./message.js";
import { declareTopology, topology as t } from "./topology.js";

const MAX_RETRIES = 3;
const connection = await amqp.connect(config.rabbitmqUrl);
const channel = await connection.createConfirmChannel();
await declareTopology(channel);
await channel.prefetch(config.prefetch);

await channel.consume(t.mainQueue, async (delivery) => {
  if (delivery === null) return;
  const retryCount = Number(delivery.properties.headers?.["x-retry-count"] ?? 0);
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
      channel.publish(t.retryExchange, t.routingKey, delivery.content, {
        ...delivery.properties,
        persistent: true,
        headers: { ...delivery.properties.headers, "x-retry-count": retryCount + 1 },
      });
      await channel.waitForConfirms();
      channel.ack(delivery);
      console.log("进入延迟重试队列", { retryCount: retryCount + 1, reason });
    } else {
      channel.publish(t.deadExchange, t.routingKey, delivery.content, {
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
  console.log("正在优雅退出……");
  await channel.close();
  await connection.close();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
