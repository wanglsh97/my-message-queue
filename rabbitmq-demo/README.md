# RabbitMQ Demo

这是一个完全独立的 RabbitMQ 项目，只启动 RabbitMQ，不会引用仓库中的其他项目。

```bash
cp .env.example .env
npm install
npm run infra:up
```

终端 A：

```bash
npm run consumer
```

终端 B：

```bash
npm run producer
```

管理台：[http://localhost:15672](http://localhost:15672)，用户名为 `admin`，密码为 `123456`。

将 `.env` 中的 `DEMO_FAILURES` 改成 `true`，第 3 条消息会经过三次延迟重试后进入 `rabbitmq-demo.tasks.dead`。使用 `npm run infra:down` 停止 RabbitMQ。
