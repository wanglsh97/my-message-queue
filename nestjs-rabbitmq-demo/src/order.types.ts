export type CreateOrderDto = {
  orderId: string;
  amount: number;
  shouldFail?: boolean;
};

export type OrderCreatedEvent = CreateOrderDto & {
  eventId: string;
  occurredAt: string;
};
