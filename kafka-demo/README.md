# Kafka Demo

这是一个完全独立的 Kafka 项目，只启动 Kafka Broker，不会引用仓库中的其他项目。

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

消息按奇偶 key 分配到分区。再启动一个 Consumer，可观察同一消费组内的分区再均衡。使用 `npm run infra:down` 停止 Kafka。

推荐使用 Node.js 20 或 22 LTS；KafkaJS 在 Node.js 24 下可能出现一次无害的 `TimeoutNegativeWarning`。
