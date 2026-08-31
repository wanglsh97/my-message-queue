import { Module } from "@nestjs/common";
import { ClientsModule, Transport } from "@nestjs/microservices";
import {
  ORDER_QUEUE,
  RABBIT_CLIENT,
  orderQueueOptions,
  rabbitUrl,
} from "./rabbit.constants.js";
import { OrderHttpController } from "./order.http-controller.js";
import { OrderMessageController } from "./order.message-controller.js";
import { OrderService } from "./order.service.js";
import { RabbitTopologyService } from "./rabbit-topology.service.js";

@Module({
  imports: [
    ClientsModule.register([
      {
        name: RABBIT_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [rabbitUrl],
          queue: ORDER_QUEUE,
          queueOptions: orderQueueOptions,
          persistent: true,
        },
      },
    ]),
  ],
  controllers: [OrderHttpController, OrderMessageController],
  providers: [OrderService, RabbitTopologyService],
})
export class AppModule {}
