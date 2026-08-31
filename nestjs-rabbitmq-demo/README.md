# NestJS + RabbitMQ Demo

该示例同时启动 HTTP API 和 NestJS RabbitMQ 微服务，演示：

- `ClientProxy.emit` + `@EventPattern` 事件模式；
- `ClientProxy.send` + `@MessagePattern` RPC 模式；
- quorum queue、持久消息、手动 ack、prefetch；
- 失败消息 `nack(requeue=false)` 后进入 DLQ；
- 用 `eventId` 表达消费者幂等意图（生产中应使用数据库唯一键，而不是内存 Set）。

```bash
npm install
npm run infra:up
npm run start:dev
```

另开终端：

```bash
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -d '{"orderId":"o-1001","amount":99}'

curl http://localhost:3001/orders/o-1001/quote

# 进入死信队列的消息
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -d '{"orderId":"o-bad","amount":99,"shouldFail":true}'
```

管理台为 <http://localhost:15672>，用户名为 `my-message-queue`，密码为 `123456`。结束后运行 `npm run infra:down`。
