# My Message Queue

这个仓库包含四个**彼此完全独立**的消息队列学习项目。根目录没有 `package.json`、Node.js 依赖、共享源码或统一 Docker Compose。

```text
my-message-queue/
├── bullmq-demo/     # 只依赖 BullMQ + Redis
├── nestjs-bullmq-demo/ # NestJS + BullMQ 6 最简集成
├── kafka-demo/      # 只依赖 KafkaJS + Kafka
└── rabbitmq-demo/   # 只依赖 amqplib + RabbitMQ
```

## 独立性约束

- 每个项目都有自己的 `package.json`、`package-lock.json`、`node_modules` 和 `tsconfig.json`。
- 每个项目都有自己的 `.env.example`、消息类型、配置代码和 Docker Compose。
- 子项目之间没有源码引用，也没有 npm workspace、monorepo 公共包或根级脚本。
- 进入哪个目录，就只安装和启动哪个消息队列的服务。

## BullMQ

```bash
cd bullmq-demo
cp .env.example .env
npm install
npm run infra:up
npm run worker
# 另开终端运行 npm run producer
```

## NestJS + BullMQ

```bash
cd nestjs-bullmq-demo
npm install
npm run infra:up
npm run start:dev
# 另开终端调用 POST http://localhost:3000/tasks
```

## Kafka

```bash
cd kafka-demo
cp .env.example .env
npm install
npm run infra:up
npm run consumer
# 另开终端运行 npm run producer
```

## RabbitMQ

```bash
cd rabbitmq-demo
cp .env.example .env
npm install
npm run infra:up
npm run consumer
# 另开终端运行 npm run producer
```

每个子项目的详细实验说明都在自己的 `README.md` 中。完成实验后，在对应子项目目录执行 `npm run infra:down`。
