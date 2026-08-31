import { Inject, Injectable, OnApplicationBootstrap } from "@nestjs/common";
import { ClientProxy } from "@nestjs/microservices";
import { randomUUID } from "node:crypto";
import { firstValueFrom } from "rxjs";
import { RABBIT_CLIENT } from "./rabbit.constants.js";
import type { CreateOrderDto, OrderCreatedEvent } from "./order.types.js";

@Injectable()
export class OrderService implements OnApplicationBootstrap {
  constructor(@Inject(RABBIT_CLIENT) private readonly client: ClientProxy) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.client.connect();
  }

  async create(dto: CreateOrderDto): Promise<OrderCreatedEvent> {
    const event: OrderCreatedEvent = {
      ...dto,
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
    };
    await firstValueFrom(this.client.emit("order.created", event));
    return event;
  }

  async quote(orderId: string): Promise<{ orderId: string; shippingFee: number }> {
    return firstValueFrom(
      this.client.send<{ orderId: string; shippingFee: number }>("order.quote", { orderId }),
    );
  }
}
