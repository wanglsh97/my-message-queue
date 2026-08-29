import { Partitioners } from "kafkajs";
import { createKafka, ensureTopic, TOPIC } from "./client.js";
import { config } from "./config.js";
import { createTaskMessage } from "./message.js";

const kafka = createKafka("kafka-demo-producer");
await ensureTopic(kafka);
const producer = kafka.producer({
  allowAutoTopicCreation: false,
  createPartitioner: Partitioners.DefaultPartitioner,
});
await producer.connect();

try {
  const messages = Array.from({ length: config.messageCount }, (_, index) => {
    const task = createTaskMessage(index + 1);
    return {
      key: String(task.payload.sequence % 2),
      value: JSON.stringify(task),
      headers: { eventType: task.type },
    };
  });
  const metadata = await producer.send({ topic: TOPIC, acks: -1, messages });
  console.log("消息已发送", metadata);
} finally {
  await producer.disconnect();
}
