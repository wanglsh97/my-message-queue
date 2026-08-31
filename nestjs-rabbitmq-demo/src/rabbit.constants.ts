export const RABBIT_CLIENT = Symbol("RABBIT_CLIENT");
export const ORDER_QUEUE = "nest.orders";
export const DEAD_EXCHANGE = "nest.orders.dead";
export const DEAD_QUEUE = "nest.orders.dead";
export const DEAD_ROUTING_KEY = "order.dead";

export const rabbitUrl = process.env.RABBITMQ_URL ?? "amqp://my-message-queue:123456@localhost:5672";

export const orderQueueOptions = {
  durable: true,
  arguments: {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": DEAD_EXCHANGE,
    "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
  },
} as const;
