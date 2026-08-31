import { Injectable, Logger, OnApplicationBootstrap } from "@nestjs/common";
import amqp from "amqplib";
import {
  DEAD_EXCHANGE,
  DEAD_QUEUE,
  DEAD_ROUTING_KEY,
  rabbitUrl,
} from "./rabbit.constants.js";

@Injectable()
export class RabbitTopologyService implements OnApplicationBootstrap {
  private readonly logger = new Logger(RabbitTopologyService.name);

  async onApplicationBootstrap(): Promise<void> {
    const connection = await amqp.connect(rabbitUrl);
    const channel = await connection.createChannel();
    try {
      await channel.assertExchange(DEAD_EXCHANGE, "direct", { durable: true });
      await channel.assertQueue(DEAD_QUEUE, {
        durable: true,
        arguments: { "x-queue-type": "quorum" },
      });
      await channel.bindQueue(DEAD_QUEUE, DEAD_EXCHANGE, DEAD_ROUTING_KEY);
      this.logger.log("死信拓扑声明完成");
    } finally {
      await channel.close();
      await connection.close();
    }
  }
}
