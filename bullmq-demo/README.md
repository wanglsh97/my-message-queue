# BullMQ Demo

这是一个完全独立的 BullMQ 项目，只依赖 Redis，不会引用仓库中的其他项目。

```bash
cp .env.example .env
npm install
npm run infra:up
```

终端 A：

```bash
npm run worker
```

终端 B：

```bash
npm run producer
```

将 `.env` 中的 `DEMO_FAILURES` 改成 `true`，可以观察第 3 个任务的三次指数退避重试。使用 `npm run clean` 清空任务，使用 `npm run infra:down` 停止 Redis。
