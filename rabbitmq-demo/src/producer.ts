import amqp from "amqplib";
import { config } from "./config.js";
import { createTaskMessage } from "./message.js";

const MAIN_EXCHANGE = "rabbitmq-demo.events";
const ROUTING_KEY = "task.created";

const connection = await amqp.connect(config.rabbitmqUrl);
const channel = await connection.createConfirmChannel();
await channel.assertExchange(MAIN_EXCHANGE, "topic", { durable: true });

try {
  for (let sequence = 1; sequence <= config.messageCount; sequence += 1) {
    const message = createTaskMessage(sequence, config.demoFailures && sequence === 3);
    channel.publish(MAIN_EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(message)), {
      persistent: true,
      contentType: "application/json",
      messageId: message.id,
      headers: { "x-retry-count": 0 },
    });
    console.log("消息已发送", { id: message.id, sequence });
  }
  await channel.waitForConfirms();
  console.log("Broker 已确认全部消息");
} finally {
  await channel.close();
  await connection.close();
}
