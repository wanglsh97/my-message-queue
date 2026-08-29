import type { Channel } from "amqplib";

export const topology = {
  mainExchange: "rabbitmq-demo.events",
  mainQueue: "rabbitmq-demo.tasks",
  routingKey: "task.created",
  retryExchange: "rabbitmq-demo.retry",
  retryQueue: "rabbitmq-demo.tasks.retry",
  deadExchange: "rabbitmq-demo.dead",
  deadQueue: "rabbitmq-demo.tasks.dead",
} as const;

export async function declareTopology(channel: Channel): Promise<void> {
  const t = topology;
  await channel.assertExchange(t.mainExchange, "topic", { durable: true });
  await channel.assertExchange(t.retryExchange, "direct", { durable: true });
  await channel.assertExchange(t.deadExchange, "direct", { durable: true });

  await channel.assertQueue(t.mainQueue, { durable: true });
  await channel.bindQueue(t.mainQueue, t.mainExchange, t.routingKey);

  await channel.assertQueue(t.retryQueue, {
    durable: true,
    arguments: {
      "x-message-ttl": 3_000,
      "x-dead-letter-exchange": t.mainExchange,
      "x-dead-letter-routing-key": t.routingKey,
    },
  });
  await channel.bindQueue(t.retryQueue, t.retryExchange, t.routingKey);

  await channel.assertQueue(t.deadQueue, { durable: true });
  await channel.bindQueue(t.deadQueue, t.deadExchange, t.routingKey);
}
