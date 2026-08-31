import { Controller, Logger } from "@nestjs/common";
import {
  Ctx,
  EventPattern,
  MessagePattern,
  Payload,
  RmqContext,
} from "@nestjs/microservices";
import type { OrderCreatedEvent } from "./order.types.js";

@Controller()
export class OrderMessageController {
  private readonly logger = new Logger(OrderMessageController.name);
  private readonly processedEventIds = new Set<string>();

  @EventPattern("order.created")
  async handleOrderCreated(
    @Payload() event: OrderCreatedEvent,
    @Ctx() rawContext: unknown,
  ): Promise<void> {
    if (!(rawContext instanceof RmqContext)) {
      throw new TypeError("当前 handler 未收到 RabbitMQ 上下文");
    }
    const context = rawContext;
    const channel = context.getChannelRef();
    const message = context.getMessage();
    try {
      if (this.processedEventIds.has(event.eventId)) {
        channel.ack(message);
        return;
      }
      if (!event.orderId || event.amount <= 0 || event.shouldFail) {
        throw new Error("订单消息校验或模拟处理失败");
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
      this.processedEventIds.add(event.eventId);
      channel.ack(message);
      this.logger.log(`订单处理成功：${event.orderId}`);
    } catch (error) {
      channel.nack(message, false, false);
      this.logger.error(`订单进入死信队列：${event.orderId}`, error);
    }
  }

  @MessagePattern("order.quote")
  quote(
    @Payload() request: { orderId: string },
    @Ctx() rawContext: unknown,
  ) {
    if (!(rawContext instanceof RmqContext)) {
      throw new TypeError("当前 handler 未收到 RabbitMQ 上下文");
    }
    rawContext.getChannelRef().ack(rawContext.getMessage());
    return { orderId: request.orderId, shippingFee: 12 };
  }
}
