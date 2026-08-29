# NestJS + BullMQ 6 最简示例

这个示例在一个 NestJS 进程中同时包含：

- `POST /tasks`：通过 `Queue` 添加 Job。
- `TaskProcessor`：通过 `WorkerHost` 消费 Job。
- Redis：持久化 Job 并协调 Queue 与 Worker。

## 运行

```bash
npm install
npm run infra:up
npm run start:dev
```

另开终端添加任务：

```bash
curl --noproxy '*' -X POST http://localhost:3000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task":"发送欢迎邮件"}'
```

接口返回类似：

```json
{"jobId":"1","state":"queued"}
```

NestJS 终端随后会输出任务开始和完成日志。

停止 Redis：

```bash
npm run infra:down
```
