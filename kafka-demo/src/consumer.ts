import { setTimeout as sleep } from "node:timers/promises";
import { createKafka, ensureTopic, TOPIC } from "./client.js";
import { config } from "./config.js";
import { parseTaskMessage } from "./message.js";

const kafka = createKafka("kafka-demo-consumer");
await ensureTopic(kafka);
const consumer = kafka.consumer({ groupId: "kafka-demo-workers-v1", allowAutoTopicCreation: false });

await consumer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
await consumer.run({
  partitionsConsumedConcurrently: config.concurrency,
  eachMessage: async ({ topic, partition, message }) => {
    if (message.value === null) throw new Error("消息 value 为空");
    const task = parseTaskMessage(message.value.toString());
    console.log("收到消息", { topic, partition, offset: message.offset, key: message.key?.toString(), task });
    await sleep(config.processingDelayMs);
    console.log("处理成功", { id: task.id });
  },
});

console.log(`Consumer 已就绪，并发分区数：${config.concurrency}`);
async function shutdown(): Promise<void> {
  console.log("正在优雅退出……");
  await consumer.disconnect();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
