import { Body, Controller, Get, Param, Post } from "@nestjs/common";
import { OrderService } from "./order.service.js";
import type { CreateOrderDto } from "./order.types.js";

@Controller("orders")
export class OrderHttpController {
  constructor(private readonly orders: OrderService) {}

  @Post()
  create(@Body() dto: CreateOrderDto) {
    return this.orders.create(dto);
  }

  @Get(":orderId/quote")
  quote(@Param("orderId") orderId: string) {
    return this.orders.quote(orderId);
  }
}
