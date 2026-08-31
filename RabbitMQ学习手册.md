# RabbitMQ 4.3 中文学习手册：Node.js 与 NestJS 从入门到面试

> 面向第一次接触消息队列、准备从前端转全栈的开发者。本文以 2026-08-30 的 [RabbitMQ 4.3.5 官方文档](https://www.rabbitmq.com/docs)为基线，以 AMQP 0-9-1、Node.js `amqplib` 和 NestJS `Transport.RMQ` 为主线。

**资料优先级**：基础概念、协议模型、核心架构、可靠性和运维结论以 RabbitMQ 官方文档为第一依据；Node.js API 以 RabbitMQ 官方 JavaScript 教程与 `amqplib` 行为为依据；NestJS 封装层再参考 NestJS 官方文档。第三方文章只适合帮助理解，不作为本手册定义 RabbitMQ 行为的依据。

## 0. 怎么使用这本手册

本手册不是官方文档的逐页翻译。官方文档同时服务应用开发者、集群管理员、插件作者和协议实现者；初学者如果平铺学习，很容易记住参数却没有形成系统。本手册把知识分为三级：

- **必须掌握**：能独立写生产者/消费者、设计路由、保证基本可靠性、排查常见问题；面试会高频询问。
- **理解原理**：知道为什么、何时使用，能够参与项目设计与生产评审。
- **知道并会查**：协议适配、跨地域、集群运维等内容，不要求第一次学习时背诵，但本手册仍完整收录知识入口。

建议用 7 天完成第一轮：

| 天数 | 学习内容 | 完成标志 |
| --- | --- | --- |
| 1 | 第 1～4 章：概念、架构、安装 | 能画出一条消息的完整路径 |
| 2 | 第 5～6 章：Node.js、六种模式 | 能解释 direct/fanout/topic 的差异 |
| 3 | 第 7～8 章：可靠性、队列能力 | 能设计 ack、confirm、幂等、DLQ |
| 4 | 第 9 章：NestJS | 能完成 HTTP → RabbitMQ → Consumer |
| 5 | 第 10～11 章：实战、运维安全 | 能写一份上线检查表 |
| 6 | 第 12～13 章：排错、技术选型 | 能解释 RabbitMQ/Kafka/BullMQ 的选择 |
| 7 | 第 14～16 章：面试题、练习、官方索引 | 闭卷回答核心面试题并跑通实验 |

仓库提供两个互不依赖、可以直接运行的项目：

- `rabbitmq-demo/`：原生 `amqplib`，包含 topic 路由、publisher confirm、手动 ack、prefetch、延迟重试和 DLQ。
- `nestjs-rabbitmq-demo/`：NestJS HTTP + RMQ 混合应用，包含事件、RPC、quorum queue、手动 ack 和 DLQ。

---

## 1. 消息队列与 RabbitMQ

### 1.1 消息队列解决什么问题

同步调用是“请求方现在等结果”：

```text
浏览器 → 订单服务 → 库存服务 → 邮件服务 → 返回
```

其中任意下游变慢或失败都会延长订单请求。消息队列把可以异步完成的动作变成“先记录要做的事，再由消费者处理”：

```text
订单服务 → Broker 持久化 order.created → 立即返回
                                      ├→ 库存消费者
                                      ├→ 邮件消费者
                                      └→ 数据分析消费者
```

它主要带来：

1. **异步**：缩短用户请求链路。
2. **解耦**：生产者只依赖消息契约，不直接依赖每个消费者。
3. **削峰**：突发流量先进入队列，消费者按能力处理。
4. **缓冲与重试**：短暂故障时保留消息，恢复后继续处理。
5. **广播和路由**：一份业务事件可以按规则交给不同系统。

代价也必须说清楚：系统从一次同步调用变成分布式异步协作，会出现重复、乱序、延迟、积压、消息契约演进、最终一致性和可观测性问题。使用 MQ 不是“自动可靠”，只是获得构建可靠系统的机制。

### 1.2 RabbitMQ 是什么

按照官方 [AMQP 0-9-1 Model Explained](https://www.rabbitmq.com/tutorials/amqp-concepts)，message broker 接收 publisher（也称 producer）发布的消息并将其路由给 consumer。AMQP 0-9-1 的完整路径是：消息发布到 exchange，exchange 按 binding 规则将消息副本分发到零个或多个 queue，broker 再把消息推送给订阅 queue 的 consumer，或由 consumer 主动拉取。

RabbitMQ 4.3 是多协议 broker，支持 AMQP 1.0、AMQP 0-9-1、MQTT、STOMP 和 RabbitMQ Stream Protocol，详见官方 [Protocols](https://www.rabbitmq.com/docs/protocols)。本手册使用官方 JavaScript 入门教程采用的 **AMQP 0-9-1 + amqplib**；学习其他协议时不能直接假设所有 AMQP 0-9-1 概念和 API 一一对应。

RabbitMQ 适合：

- 任务队列、业务事件、微服务解耦；
- 灵活路由、低到中等延迟；
- 需要逐条确认、重试、死信和 RPC；
- 一个消息被多个业务订阅者分别处理。

它不是数据库；不要把队列当长期业务事实存储。它也不是所有场景的最佳事件日志，大规模长期保留和历史回放通常更适合 Kafka；单 Node 应用的后台任务也可能更适合 BullMQ。

### 1.3 命令式消息与事件

- **命令**：希望某个处理者做事，如 `payment.capture`。通常一个逻辑消费者，命名用祈使或动作语义。
- **事件**：已经发生的事实，如 `order.created.v1`。可以有多个独立订阅者，命名用过去式。
- **查询/RPC**：需要返回结果，如 `order.quote`。它增加时间耦合，不应把所有同步 HTTP 调用机械改成 RabbitMQ RPC。

面试表达：队列不是架构目的，目标是划分责任、缩短同步链路、控制故障传播；先确认业务允许最终一致性，再选择异步消息。

---

## 2. 核心架构：官方 AMQP 0-9-1 模型

### 2.1 官方模型的五个结论

以下五点直接对应 RabbitMQ 官方 [AMQP 0-9-1 模型说明](https://www.rabbitmq.com/tutorials/amqp-concepts)：

1. Publisher 把消息发布到 exchange，而 exchange 根据类型和 binding 将消息路由到零个或多个 queue。
2. Queue 存储供应用消费的消息；exchange 负责路由，不是消息积压容器。
3. Consumer 推荐通过订阅使用 push API 接收消息；轮询式 pull API 在多数场景效率较低。
4. Broker 把 payload 当作不透明字节数组；JSON、字段含义、Schema 与兼容性由应用负责。
5. Queue、exchange 和 binding 合称 AMQP entities，主要由应用通过协议操作声明，因此应用必须避免同名资源定义冲突。

把官方模型画成一条完整消息路径：

```text
Publisher
   │ publish(exchange, routingKey, body, properties)
   ▼
Connection ── Channel
   │
   ▼
Exchange ──按类型、routing key、binding 路由──┐
   │                                         │
   ▼                                         ▼
 Queue A                                  Queue B
   │ basic.deliver                           │
   ▼                                         ▼
Consumer A1 / A2                         Consumer B
   │
   └── ack / nack / reject ──→ RabbitMQ
```

### 2.2 核心实体与官方依据

| 名称 | 官方模型中的作用 | 易错点 | 官方依据 |
| --- | --- | --- | --- |
| Producer/Publisher | 发布消息的应用 | 通常发布到 exchange，不是“绕过交换机直写队列” | [Publishers](https://www.rabbitmq.com/docs/publishers) |
| Consumer | 注册订阅并处理 queue delivery 的应用 | 同 queue 多 consumer 通常竞争消费，不是每个都收到 | [Consumers](https://www.rabbitmq.com/docs/consumers) |
| Broker/Node | 接收、路由、存储和交付消息的 RabbitMQ 服务进程 | cluster 是多个 node 的逻辑组，不等于自动复制所有 queue 内容 | [AMQP Model](https://www.rabbitmq.com/tutorials/amqp-concepts) |
| Connection | AMQP 0-9-1 客户端到 broker 的长生命周期连接，通常基于 TCP，可用 TLS | 不要每条消息新建 connection | [Connections](https://www.rabbitmq.com/docs/connections) |
| Channel | connection 上复用的轻量逻辑连接，客户端协议操作都在 channel 上执行 | 不能脱离 connection 存在；不宜无限创建或无约束并发共享 | [Channels](https://www.rabbitmq.com/docs/channels) |
| Exchange | 接收发布并按类型与 binding 路由到 queue、stream 或其他 exchange | 可以路由到零个目标；exchange 不负责积压消息 | [Exchanges](https://www.rabbitmq.com/docs/exchanges) |
| Queue | 有序消息集合，保存并向 consumer 交付消息 | 通常 FIFO，但 priority、requeue 和多 consumer 会影响观察顺序 | [Queues](https://www.rabbitmq.com/docs/queues) |
| Binding | exchange 使用的路由规则，连接 source 与 destination | binding key 与发布 routing key 不是同一个属性 | [Bindings](https://www.rabbitmq.com/tutorials/amqp-concepts#bindings) |
| Routing key | publisher 随消息提供、供部分 exchange 类型匹配的路由属性 | fanout 忽略它；headers exchange 主要依据 headers | [Exchange Types](https://www.rabbitmq.com/docs/exchanges#exchange-types) |
| Virtual host | 隔离用户、exchange、queue 等 AMQP entities 的逻辑环境 | 不是操作系统虚拟机；客户端在连接协商时选择一个 vhost | [Virtual Hosts](https://www.rabbitmq.com/docs/vhosts) |

### 2.3 Connection、Channel 与心跳

官方说明 AMQP 0-9-1 connection 通常是长生命周期连接，channel 则是共享同一 TCP connection 的轻量连接。基于此，应用应维护少量长连接，在连接上创建若干长生命周期 channel；发布和消费最好使用不同 channel，高并发发布可以做受控 channel 池。连接握手和 TLS 成本高，channel 更轻，但 channel 也会消耗 broker 资源。

RabbitMQ 与客户端会协商 heartbeat。它能更快发现“TCP 看似存在、对端实际失联”的半开连接。过低容易受短暂拥塞影响产生误判，禁用又会延迟故障发现；生产一般采用客户端/平台合理默认值并结合 TCP keepalive。断连恢复必须考虑：退避重连、重新声明 topology、重新订阅、未确认发布如何补偿，以及重连期间的流量背压。

官方：[Connections](https://www.rabbitmq.com/docs/connections)、[Channels](https://www.rabbitmq.com/docs/channels)、[Heartbeats](https://www.rabbitmq.com/docs/heartbeats)。

### 2.4 Virtual host、用户与权限

按照官方 [Virtual Hosts](https://www.rabbitmq.com/docs/vhosts)，vhost 为 connection、exchange、queue、binding、用户权限、policy、runtime parameter 等提供逻辑分组与隔离。资源名称只在所属 vhost 内唯一，因此不同 vhost 中可以有同名 queue，但它们不是同一资源。生产通常按环境或系统边界拆分 vhost，不给应用使用 `/` 和管理员账号。

权限由 configure、write、read 三个资源名正则表达式组成：

- configure：声明/删除 exchange、queue、binding 等；
- write：向匹配资源发布；
- read：从匹配资源消费/读取。

建议每个服务使用独立最小权限账号，凭据从密钥系统注入，生产使用 TLS（`amqps`，通常端口 5671）。权限语义详见官方 [Access Control](https://www.rabbitmq.com/docs/access-control)。

### 2.5 声明是幂等的，但属性必须等价

官方 [Queues](https://www.rabbitmq.com/docs/queues#declaration-and-property-equivalence) 规定：`queue.declare` 在资源不存在时创建，存在且属性一致时成功。若同名 queue 已存在但 durable、exclusive、queue type 或不可变参数不一致，会产生 channel-level `PRECONDITION_FAILED (406)`。`amqplib` 的 `assertQueue` 对应这一协议声明过程。这是开发中“明明能连上但一声明就断开”的常见原因。

动态可调整的 TTL、DLX、长度上限等优先用 **policy** 管理，不要全部硬编码为 `x-arguments`；客户端参数通常优先于普通 policy，operator policy 用来给资源使用设置平台护栏。

---

## 3. Exchange、Binding 与路由

本章的类型定义和匹配规则以官方 [Exchanges](https://www.rabbitmq.com/docs/exchanges) 与 [AMQP 0-9-1 Model Explained](https://www.rabbitmq.com/tutorials/amqp-concepts#exchanges-and-exchange-types) 为准。

### 3.1 四种基础交换机

| 类型 | 匹配方式 | 典型用途 | 示例 |
| --- | --- | --- | --- |
| direct | binding key 与 routing key 完全相等；同 key 可匹配多个 queue | 按明确类别路由 | `payment.success` |
| fanout | 忽略 routing key，把消息副本路由到所有已绑定目标 | 广播通知 | 缓存刷新、日志广播 |
| topic | 点分段模式；`*` 恰好一段，`#` 零到多段 | 领域事件与灵活订阅 | `order.*`、`order.#` |
| headers | 忽略 routing key，根据 headers 匹配；`x-match=all/any` | 路由条件不适合字符串层级 | 地区+格式+版本 |

topic 例子：

```text
binding: order.*       匹配 order.created，不匹配 order.eu.created
binding: order.#       匹配 order、order.created、order.eu.created
binding: *.created     匹配 order.created，不匹配 order.eu.created
```

### 3.2 默认交换机

根据官方 [Default Exchange](https://www.rabbitmq.com/docs/exchanges#default-exchange)，AMQP 0-9-1 的默认 exchange 名称是空字符串 `""`。每个 queue 声明时自动以“queue 名作为 routing key”绑定到它，因此：

```ts
channel.sendToQueue("email.tasks", body);
// 本质等价于：
channel.publish("", "email.tasks", body);
```

这只是方便机制，不代表 RabbitMQ 中消息可以完全不经过 exchange。

### 3.3 无法路由的消息

消息到达 exchange 却没有匹配 binding 时，默认会被丢弃。它不会因为“发布成功”就自动进入某个 DLQ。处理方式：

1. 发布时启用 `mandatory: true` 并监听 `return`；
2. 给 exchange 配置 alternate exchange（AE）收集 unroutable message；
3. 用 publisher confirm 确认 broker 处理发布。注意 confirm 与 routable 是两个问题。

DLX 处理“消息已经进过队列，后来死亡”；AE 处理“exchange 一开始就无法路由”。这是高频面试区别。

### 3.4 其他交换能力（知道并会查）

- exchange-to-exchange binding：一个交换机绑定到另一个，扩展路由图，不是应用层重新发布。
- Local Random Exchange：把消息随机路由到一个本地可用目标，适合部分本地性场景。
- Modulus Hash Exchange：按 key 的哈希取模稳定分发，RabbitMQ 4.3 文档新增的内置/扩展路由能力之一。
- 插件还可提供 consistent-hash、recent-history、random 等交换机类型。
- 系统交换机包括 `amq.rabbitmq.log`、`amq.rabbitmq.event`、`amq.rabbitmq.trace`，用于日志、内部事件与追踪。

官方：[Exchanges](https://www.rabbitmq.com/docs/exchanges)、[Alternate Exchanges](https://www.rabbitmq.com/docs/ae)、[E2E Bindings](https://www.rabbitmq.com/docs/e2e)。

---

## 4. 安装、管理台与第一条消息

### 4.1 Docker 启动

本仓库两个 RabbitMQ 示例均使用 RabbitMQ 4.3 management 镜像。一次只启动一个，避免 5672/15672 端口冲突：

```bash
cd rabbitmq-demo
cp .env.example .env
npm install
npm run infra:up
```

常用端口：

| 端口 | 用途 |
| --- | --- |
| 5672 | AMQP 0-9-1/AMQP 1.0 明文连接 |
| 5671 | AMQP TLS（需配置） |
| 15672 | Management UI 和 HTTP API，非 AMQP |
| 15692 | Prometheus 指标（启用对应插件后） |
| 25672、4369 | 节点间/CLI 分布式通信，不应暴露给业务客户端 |
| 5552 | RabbitMQ Stream Protocol 默认端口 |

打开 <http://localhost:15672>，使用用户名 `my-message-queue`、密码 `123456` 登录。重点观察：

- Overview：消息速率、连接、channel、queue 总览；
- Connections/Channels：应用是否复用连接、是否泄漏 channel；
- Exchanges：绑定和 publish in/out；
- Queues and Streams：Ready、Unacked、consumer、内存/磁盘、DLX；
- Admin：用户、vhost、权限、policy、runtime parameter。

### 4.2 最小 Node.js 生产者与消费者

```ts
import amqp from "amqplib";

const connection = await amqp.connect("amqp://my-message-queue:123456@localhost:5672");
const channel = await connection.createChannel();
await channel.assertQueue("hello", { durable: true });

channel.sendToQueue("hello", Buffer.from(JSON.stringify({ text: "你好" })), {
  persistent: true,
  contentType: "application/json",
});
```

```ts
await channel.consume("hello", async (message) => {
  if (!message) return; // consumer 被服务端取消时可能收到 null
  try {
    const data = JSON.parse(message.content.toString());
    console.log(data);
    channel.ack(message);
  } catch (error) {
    channel.nack(message, false, false); // 不 requeue；有 DLX 则死信
  }
}, { noAck: false });
```

`Buffer` 是消息体；`contentType`、`messageId`、`type`、`timestamp`、`correlationId`、`replyTo`、`headers`、`expiration`、`priority` 等是 properties。RabbitMQ 不理解你的 JSON 业务结构，契约、校验和版本兼容由应用负责。

### 4.3 常用 CLI

容器内执行：

```bash
docker exec rabbitmq-demo-rabbitmq rabbitmq-diagnostics -q ping
docker exec rabbitmq-demo-rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
docker exec rabbitmq-demo-rabbitmq rabbitmqctl list_connections name channels state
docker exec rabbitmq-demo-rabbitmq rabbitmqctl list_channels connection number consumer_count messages_unacknowledged prefetch_count
docker exec rabbitmq-demo-rabbitmq rabbitmqctl list_exchanges name type durable
docker exec rabbitmq-demo-rabbitmq rabbitmqctl list_bindings source_name destination_name routing_key
```

`rabbitmqctl` 管理 broker；`rabbitmq-diagnostics` 做诊断和健康检查；`rabbitmq-plugins` 管理插件；`rabbitmq-queues` 管理 quorum queue；`rabbitmq-streams` 管理 streams；`rabbitmqadmin` 是基于 HTTP API 的管理 CLI。

---

## 5. 六种经典消息模式：Node.js 与 NestJS 对照

RabbitMQ 官方 AMQP 0-9-1 教程有 Hello World、Work Queues、Publish/Subscribe、Routing、Topics、RPC 和 Publisher Confirms。不要背七个程序，重点是拓扑变化。

### 5.1 Hello World：一个队列

```text
P → [hello] → C
```

Node.js 使用 `sendToQueue`/`consume`，见 4.2。NestJS 中，`ClientsModule` 配置 `queue: "hello"`，使用 `client.emit("hello", payload)` 与 `@EventPattern("hello")`。

### 5.2 Work Queue：竞争消费者

```text
                    ┌→ C1
P → [task.queue] ───┤
                    └→ C2
```

同一个队列的多个 consumer 竞争消息；每条消息通常只交给其中一个。这能横向扩容 worker。关键配置：

```ts
await channel.prefetch(5);
await channel.consume("task.queue", handler, { noAck: false });
```

`prefetch=5` 表示 RabbitMQ 最多给该 consumer 保留 5 条尚未 ack 的 delivery（RabbitMQ 对 AMQP 0-9-1 QoS 有按 consumer 应用的扩展语义）。如果任务耗时差异很大，小 prefetch 更公平；如果任务很快且网络延迟明显，适当增大能提高吞吐。它是流量窗口，不是并发执行器；Node handler 是否并发还取决于代码。

NestJS：启动同一服务的多个实例，监听相同 queue，设置 `noAck: false` 和 `prefetchCount`。

### 5.3 Publish/Subscribe：每个订阅者都收到

```text
                   ┌→ [email.queue] → Email C
P → (fanout ex) ───┤
                   └→ [audit.queue] → Audit C
```

每个业务订阅者必须拥有自己的队列。fanout 把消息复制到每个绑定队列；不是让两个消费者监听同一队列。

Node topology：

```ts
await channel.assertExchange("user.events", "fanout", { durable: true });
await channel.assertQueue("email.user-events", { durable: true });
await channel.assertQueue("audit.user-events", { durable: true });
await channel.bindQueue("email.user-events", "user.events", "");
await channel.bindQueue("audit.user-events", "user.events", "");
channel.publish("user.events", "", body, { persistent: true });
```

NestJS Transport 可以配置 `exchange`、`exchangeType: "fanout"`；复杂、多队列拓扑建议由部署定义、独立 topology bootstrap 或直接 `amqplib` 声明，而不是让业务 handler 隐式决定整个平台拓扑。

### 5.4 Routing：direct 精确路由

```text
P → (direct logs) --info----→ [info.queue]
                  --error---→ [error.queue]
```

```ts
await channel.assertExchange("logs", "direct", { durable: true });
await channel.bindQueue("error.logs", "logs", "error");
channel.publish("logs", "error", Buffer.from("数据库失败"));
```

一个 queue 可以用多个 binding key 绑定；同一 key 也可以绑定多个 queue。

### 5.5 Topics：模式路由

```text
P → (topic domain.events)
        ├─ order.* → [billing]
        └─ order.# → [analytics]
```

本仓库 `rabbitmq-demo/src/topology.ts` 使用 topic exchange 和 `task.created`。NestJS 12 官方 RMQ transporter 可以设置 `wildcards: true`、`exchange` 和 `exchangeType: "topic"`，然后用 `@EventPattern("order.#")` 订阅。

命名建议：`<domain>.<entity>.<event>.v<version>` 或团队统一的较短层级，如 `order.created.v1`。不要把用户输入原样拼进无限基数 routing key。

### 5.6 RPC：请求/响应

```text
Client -- request(correlationId, replyTo) → [rpc.queue] → Server
Client ←-------------- response ---------- reply queue
```

客户端生成唯一 `correlationId`，指定 `replyTo`，服务端处理后向 reply queue 发布并带回相同 correlation ID，客户端据此匹配 Promise。RabbitMQ 的 Direct Reply-To 使用伪队列 `amq.rabbitmq.reply-to`，减少大量临时 reply queue 的元数据开销，但响应采用 at-most-once 语义；客户端掉线时响应可能丢失。

NestJS 已封装 request/reply：

```ts
// client：send 返回 cold Observable，必须订阅或 firstValueFrom
await firstValueFrom(client.send("order.quote", { orderId: "o-1" }));

// server
@MessagePattern("order.quote")
quote(@Payload() data: { orderId: string }, @Ctx() context: RmqContext) {
  context.getChannelRef().ack(context.getMessage()); // noAck:false 时 RPC 也要确认
  return { orderId: data.orderId, shippingFee: 12 };
}
```

面试提醒：RPC over MQ 仍是同步依赖，应设置超时、关联 ID、容量限制和降级；如果只是普通查询，HTTP/gRPC 往往更直接。

### 5.7 Publisher Confirms

```ts
const channel = await connection.createConfirmChannel();
channel.publish(exchange, key, body, { persistent: true });
await channel.waitForConfirms();
```

逐条等待 confirm 最简单但吞吐低；生产常用批量 confirm 或异步追踪 outstanding sequence number。事务 `tx.select/commit` 也能提供发布原子性，但性能成本高，通常优先 confirm。

---

## 6. 可靠性：RabbitMQ 面试与生产的核心

### 6.1 两种确认解决不同问题

```text
Publisher --publish--> RabbitMQ --delivery--> Consumer
          <--confirm--          <--ack/nack--
```

- **publisher confirm**：broker 告诉 publisher 发布处理结果；它不知道 consumer 是否完成业务。
- **consumer ack**：consumer 告诉 broker 某次 delivery 已处理；它不知道原 publisher。

confirm、ack 都是 channel 级协议能力。不要回答“开启 ack 就保证生产者不丢消息”。

### 6.2 ack、nack、reject 与 requeue

- `ack(message)`：成功，RabbitMQ 可以移除该 delivery。
- `nack(message, allUpTo, requeue)`：失败，可批量；RabbitMQ 扩展。
- `reject(message, requeue)`：失败，只能单条。
- channel/connection 在未 ack 时关闭：delivery 自动 requeue，之后可能带 `redelivered=true`。

`requeue=true` 如果错误不可恢复，会形成高速死循环并消耗 CPU/网络。推荐区分：

| 错误 | 处理 |
| --- | --- |
| 短暂网络/依赖超时 | 有上限、有退避的延迟重试 |
| 数据格式错误、业务永久拒绝 | 不重试，进入 DLQ/告警 |
| 进程意外退出 | 未 ack 自动重新投递 |

ack 必须在收到 delivery 的同一个 channel 上发送；重复 ack 或错误 delivery tag 会关闭 channel。

### 6.3 durable、persistent、confirm 缺一不可

- durable exchange/queue：拓扑在 broker 重启后仍存在；
- persistent message（delivery mode 2）：要求 broker 按持久消息处理；
- publisher confirm：publisher 知道 broker 是否确认接收/持久化到该队列类型要求的程度；
- replicated queue：单节点磁盘损坏仍可能丢失，生产关键数据应考虑 quorum queue。

仅设置 `persistent: true` 不是绝对保证。消息尚在网络/客户端缓冲、路由不到持久队列、没有等待 confirm、单节点磁盘损坏，仍可能丢失。

### 6.4 投递语义

| 语义 | 做法 | 结果 |
| --- | --- | --- |
| at-most-once | 自动 ack 或处理前 ack | 不重复，但失败可能丢 |
| at-least-once | confirm + 手动处理后 ack + 失败重投 | 不轻易丢，但可能重复 |
| exactly-once effect | at-least-once + 业务幂等/去重/事务设计 | 追求业务效果一次，不是 broker 魔法开关 |

为什么会重复：消费者业务已提交，但 ack 在网络中丢失或进程在 ack 前崩溃，RabbitMQ 只能重新投递。生产者也可能因 confirm 超时而重发一条其实已经成功的消息。

### 6.5 消费者幂等

常见方案：

1. 每条事件有稳定唯一 `messageId/eventId`；
2. 消费者数据库建 `processed_messages(consumer, message_id)` 唯一索引；
3. 在同一个数据库事务中插入去重记录并修改业务数据；
4. 唯一冲突代表已处理，可直接 ack；
5. 外部副作用使用服务端幂等键，或先记录本地状态再可靠驱动。

伪代码：

```ts
await db.transaction(async (tx) => {
  await tx.processedMessage.create({ consumer: "inventory", messageId }); // 唯一键
  await tx.inventory.decrement({ sku, quantity });
});
channel.ack(message);
```

内存 `Set` 只适合教学，进程重启和多实例时不可靠。

### 6.6 发布端 Outbox

“先提交数据库再 publish”会在中间崩溃时漏消息；“先 publish 再提交数据库”会让消费者看到最终回滚的数据。Transactional Outbox：

1. 在同一数据库事务里写业务表和 outbox 表；
2. 独立 relay 扫描/订阅 outbox，发布到 RabbitMQ；
3. 等 confirm 后标记已发布；
4. relay 失败可重试，重复由消费者幂等处理。

这实现数据库事实与“最终会发布”之间的一致性，不代表消息绝不重复。可用 CDC 替代轮询，但复杂度更高。

### 6.7 顺序

队列入队通常 FIFO，但以下情况改变消费者观察到的顺序：多个竞争 consumer 并发、不同处理耗时、prefetch、redelivery/requeue、priority、多个 publisher/channel、分片。需要某个聚合根严格顺序时，可以：

- routing key 按 `orderId` 一致哈希到固定分片队列；
- 每个分片用单活/单消费者串行处理；
- 消息带 entity version，消费者拒绝旧版本或暂存缺口；
- 接受吞吐、可用性和运维复杂度的代价。

### 6.8 背压与资源告警

`channel.publish()` 返回 `false` 表示 Node 可写缓冲超过 high-water mark，应等待 `drain`；这不是 publisher confirm。RabbitMQ 内存或磁盘达到 alarm 阈值时会阻塞发布连接，客户端可收到 `blocked`/`unblocked` 通知。消费者连接不一定同样阻塞，因此发布与消费最好分开 connection，避免消费者被同一受阻连接影响。

持续积压不是靠“无限磁盘”解决：应限制队列长度/字节、设置消息 TTL、扩容或优化 consumer、让入口降速，并对 oldest message age 和 Ready 增长率告警。

官方：[Reliability](https://www.rabbitmq.com/docs/reliability)、[Confirms and Acks](https://www.rabbitmq.com/docs/confirms)、[Flow Control](https://www.rabbitmq.com/docs/flow-control)、[Blocked Connections](https://www.rabbitmq.com/docs/connection-blocked)。

---

## 7. Queue、TTL、死信、优先级与重试

### 7.1 队列属性

- `durable`：broker 重启后队列定义保留；
- `exclusive`：只允许声明它的 connection 使用，连接关闭即删除；
- `autoDelete`：至少有过 consumer，最后一个 consumer 离开后删除；
- server-named：传空名称由 broker 生成，适合临时订阅/RPC；
- arguments/policy：queue type、TTL、长度、DLX、优先级等。

exclusive queue 通常配合 server-generated name，避免断线重连时旧队列删除与新声明之间的竞态。

### 7.2 Classic、Quorum、Stream 如何选

| 类型 | 核心模型 | 适用 | 关键限制/特点 |
| --- | --- | --- | --- |
| classic queue | 单副本普通队列 | 临时、低重要度或单节点场景 | 集群内消息内容不自动复制；旧 mirrored classic queues 自 4.0 已移除 |
| quorum queue | Raft 复制的持久队列 | 生产关键任务/事件，默认优先考虑 | 通常 3 或 5 副本且需多数在线；不适合临时 exclusive；磁盘与复制成本更高 |
| stream | 追加日志、基于 offset 非破坏读取 | 大吞吐、保留、回放、多个消费者从不同位置读 | 总是落盘、按保留策略删除；消费模型不同于普通 queue |
| super stream | 分区后的多个 stream | 单 stream 吞吐不足且需按 key 保序 | 客户端需理解分区和路由 |

RabbitMQ 4.3 中，quorum queue 是需要复制、高可用和数据安全时的推荐队列类型；confirm 在数据写入并刷到法定多数副本后发出。stream confirm 在复制到多数副本后发出，但依赖操作系统页缓存的后续刷盘；极致数据安全更偏 quorum queue。

quorum queue 使用多数派，因此 3 节点可容忍 1 个失败，5 节点可容忍 2 个。2 节点无法在分区时形成可靠多数，不推荐。队列可从任意 cluster node 访问，操作会透明路由到 leader。

### 7.3 TTL

- per-queue message TTL：队列中的每条消息最多保留多久；
- per-message expiration：publisher 给单条消息设置字符串毫秒值；
- queue expires：队列在“未使用”一段时间后删除。

同一消息路由到多个队列后，可在每个队列独立过期。整条 queue 过期删除时，其中消息不会因此被 dead-letter。对可动态调整的 TTL 优先使用 policy。

### 7.4 长度限制与 overflow

可按消息数或总字节设置 `max-length`/`max-length-bytes`。达到上限时默认从头部丢消息并可 dead-letter；也可配置拒绝新发布（`reject-publish` 或相关模式），配合 publisher confirms 感知。operator policy 可给应用设资源上限。

### 7.5 Dead Letter Exchange

消息在以下情况下从队列 dead-letter：

1. consumer `reject/nack` 且 `requeue=false`；
2. 消息 TTL 过期；
3. 队列超过长度限制而丢弃；
4. quorum queue 超过 delivery limit。

DLX 是普通 exchange，不是特殊队列。原队列配置 DLX 与可选 dead-letter routing key；DLX 再绑定 DLQ。RabbitMQ 会记录 `x-death` 等死亡信息。优先通过 policy 配 DLX，因为硬编码参数变更常要删队列重建。

```ts
await channel.assertExchange("orders.dlx", "direct", { durable: true });
await channel.assertQueue("orders", {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": "orders.dlx",
    "x-dead-letter-routing-key": "order.dead",
  },
});
```

DLQ 必须有责任人、告警、查看工具、修复/重放流程。把坏消息悄悄放进 DLQ 不是完成可靠性设计。

### 7.6 延迟重试

经典且通用的 TTL + DLX 模式：

```text
main queue --失败后发布--> retry queue(TTL=3s)
   ↑                            │ TTL 到期，DLX
   └────────────────────────────┘
失败超过上限 → dead exchange → DLQ
```

本仓库 `rabbitmq-demo/src/consumer.ts` 在把新重试消息获得 confirm 后才 ack 原消息，避免“ack 后重发失败”造成丢失，并用 `x-retry-count` 限制次数。运行：

```bash
cd rabbitmq-demo
cp .env.example .env
# 把 DEMO_FAILURES 改成 true
npm run infra:up
npm run consumer
# 另一终端
npm run producer
```

生产建议指数退避（例如 5s、30s、5m）加少量随机抖动，避免依赖恢复时惊群。可以建立多个 TTL bucket queue，或使用 RabbitMQ 4.3 quorum queue 的 delayed retries 能力；第三方 delayed-message exchange 插件也常见，但要评估版本支持与运维成本。

### 7.7 Priority、Single Active Consumer 和消费者优先级

- classic queue 用 `x-max-priority` 声明，官方建议 2～4 等个位数级别；每一级有资源成本。
- RabbitMQ 4.3 quorum queue 原生支持 0～31 共 32 个严格优先级，无需 `x-max-priority`；超范围会截断。
- prefetch 已把低优先级消息交付成 unacked 后，高优先级无法穿越这些在途消息，因此优先队列通常配合适中的 prefetch。
- Single Active Consumer（SAC）让一个队列同时只有一个活动消费者，失效时切换，适合保序/单活。
- consumer priority 决定优先向哪个 consumer group 交付，和 message priority 不是一回事。

不要滥用 priority 代替容量治理；低优先级消息可能饥饿。RabbitMQ 3.12 以后不再支持旧的 lazy queue 模式，相关参数会被忽略，现代 classic queue 已采用相似的磁盘优先行为。

官方：[Queues](https://www.rabbitmq.com/docs/queues)、[Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)、[Streams](https://www.rabbitmq.com/docs/streams)、[TTL](https://www.rabbitmq.com/docs/ttl)、[DLX](https://www.rabbitmq.com/docs/dlx)、[Priority](https://www.rabbitmq.com/docs/priority)、[Queue Length](https://www.rabbitmq.com/docs/maxlength)。

---

## 8. 消息契约、兼容性与应用设计

### 8.1 推荐事件信封

```ts
type EventEnvelope<T> = {
  id: string;                 // 全局 event/message ID，用于幂等与追踪
  type: string;               // order.created
  version: number;            // 契约版本
  source: string;             // order-service
  occurredAt: string;         // 业务事实发生时间，ISO 8601 UTC
  correlationId?: string;     // 串联一次业务流程
  causationId?: string;       // 导致本事件的上一事件
  traceparent?: string;       // W3C Trace Context
  payload: T;
};
```

同时设置 AMQP properties：`messageId=id`、`type`、`contentType=application/json`、`timestamp`、必要的 `correlationId`。不要把密码、token、身份证等敏感数据塞进消息；消息可能进入日志、DLQ、备份和管理台。

### 8.2 契约演进

- 优先做向后兼容的新增可选字段；消费者忽略未知字段。
- 不复用字段改变语义，不随意重命名/改类型/删除必填字段。
- 破坏性变化使用新 `type`/版本或新 routing key，并给迁移窗口。
- 入口做 schema validation（JSON Schema、Zod、class-validator 等），错误消息直接 DLQ，不无限重试。
- 生产者和消费者使用契约测试；共享 TypeScript type 只能提供编译期帮助，不能校验网络中的真实字节，也不能服务非 TS 消费者。

### 8.3 消息粒度

消息应包含消费者完成动作所需的稳定数据，但不要复制整个数据库对象。只放 ID 会导致消费者回调生产者查询，重新引入时间耦合；放全部内部模型又会强耦合和泄漏。事件携带“事实快照中的必要字段”，命令携带执行命令所需参数。

大文件不要直接塞 RabbitMQ。把文件存对象存储，消息只包含受控、短期可访问的对象标识和校验信息。大消息增加内存、复制、网络和恢复成本。

---

## 9. NestJS + RabbitMQ 完整 Demo

### 9.1 为什么先学 amqplib 再学 Nest Transport

`@nestjs/microservices` 提供 `ClientProxy`、`@EventPattern`、`@MessagePattern`、序列化和 RPC，适合 Nest 服务间统一开发体验。它不会替你消除重复投递、事务边界、契约兼容和积压，也不会让 exchange/queue/binding 的概念失效。复杂拓扑、非 Nest 消费者、confirm 的精细控制或批量发布场景，直接封装 `amqplib`/`amqp-connection-manager` 往往更透明。

### 9.2 运行项目

```bash
cd nestjs-rabbitmq-demo
npm install
npm run infra:up
npm run start:dev
```

另开终端：

```bash
# 事件成功
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -d '{"orderId":"o-1001","amount":99}'

# RPC request/reply
curl http://localhost:3001/orders/o-1001/quote

# 模拟失败，nack(requeue=false) 后进入 nest.orders.dead
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -d '{"orderId":"o-bad","amount":99,"shouldFail":true}'
```

### 9.3 服务端配置

```ts
app.connectMicroservice<MicroserviceOptions>({
  transport: Transport.RMQ,
  options: {
    urls: [rabbitUrl],
    queue: "nest.orders",
    queueOptions: {
      durable: true,
      arguments: {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "nest.orders.dead",
        "x-dead-letter-routing-key": "order.dead",
      },
    },
    noAck: false,
    prefetchCount: 5,
  },
});
```

`noAck:false` 才能手动确认。Nest handler 通过 `RmqContext` 取得原始 message 和 channel：

```ts
@EventPattern("order.created")
async handle(@Payload() event: OrderCreatedEvent, @Ctx() context: RmqContext) {
  const channel = context.getChannelRef();
  const message = context.getMessage();
  try {
    await this.applicationService.execute(event);
    channel.ack(message);
  } catch (error) {
    channel.nack(message, false, false);
  }
}
```

完整代码见 `nestjs-rabbitmq-demo/src/`。示例用内存 `Set` 表达幂等意图，生产必须改为数据库唯一键。`RabbitTopologyService` 只声明 DLX/DLQ；更成熟的团队通常用基础设施定义或专门部署步骤管理 topology 和 policies。

### 9.4 ClientProxy 的两个入口

```ts
// 事件：hot Observable，立即尝试发送；不等待业务响应
await firstValueFrom(client.emit("order.created", event));

// RPC：cold Observable，不订阅就不发送；等待响应
const quote = await firstValueFrom(client.send("order.quote", request));
```

`emit` 完成表示 Nest transporter 的发送流程完成，不等于所有业务 consumer 成功处理。需要业务完成通知时设计单独事件/状态机，不要把 consumer ack 误当成跨服务业务响应。

### 9.5 Nest 工程建议

1. 配置使用 `ConfigModule` 和密钥注入，禁止源码硬编码生产凭据。
2. producer client 在 `OnApplicationBootstrap` 调 `connect()`，让启动失败可见；实现 readiness。
3. DTO 在 HTTP 和消息入口分别校验；消息 handler 中捕获并分类错误。
4. channel ack/nack 只由接收该 delivery 的 handler 决定。
5. 为 handler 记录 messageId、correlationId、routing key、重试次数和耗时，但不记录敏感 payload。
6. graceful shutdown：停止接新流量，等待在途任务，关闭 microservice/client；超时后未 ack 消息会重新入队。
7. Nest 抽象不够时，建立独立 `RabbitModule` 管理 connection、confirm channel、topology 与 metrics，而非在每个 service 中 `amqp.connect()`。

---

## 10. 实战：电商订单事件驱动设计

### 10.1 需求

用户提交订单后立即返回；库存、支付准备、通知、积分和分析异步执行。要求不漏关键事件，允许至少一次，失败可定位和重放。

### 10.2 拓扑

```text
order-service
  └─ outbox relay → (topic: commerce.events)
                       routing key: order.created.v1
                       ├→ inventory.order-created (quorum)
                       ├→ notification.order-created (quorum)
                       └→ analytics.order-created (stream 或 queue)

inventory 失败 → retry buckets → inventory.order-created
永久失败     → inventory.order-created.dlq → 告警/人工修复/受控重放
```

每个业务订阅者一个队列，所以相互独立；同一订阅者的多实例竞争消费，实现水平扩容。命名中加入环境/vhost 由团队规范决定，不要让开发与生产共用 vhost。

### 10.3 完整发布流程

1. HTTP 层生成 `orderId` 和 trace/correlation ID。
2. 数据库事务写 `orders` 与 `outbox_events`。
3. 返回 `202 Accepted` 或包含当前状态的 `201`，明确后续为最终一致。
4. outbox relay 使用 confirm channel 发布 persistent event。
5. `mandatory` return/AE 发现无法路由；confirm 后标记 outbox 已发送。
6. 消费者校验契约，以 `(consumer_name,event_id)` 幂等处理，在业务事务提交后 ack。
7. 可恢复错误延迟重试，永久错误 DLQ 并告警。
8. 通过后续事件更新订单状态；前端轮询、SSE 或 WebSocket 获取状态。

### 10.4 Saga 与补偿

跨服务业务不使用分布式数据库事务时，可用 Saga：

- choreography：服务根据事件自行推进，简单流程耦合低，但长流程难观察；
- orchestration：协调器显式发命令、收结果、触发补偿，流程清晰但协调器更重要。

补偿不是数据库 rollback，例如付款成功但库存失败，补偿可能是退款。每个步骤和补偿都应幂等，状态机拒绝非法/旧状态转换。

### 10.5 上线前验收

- 杀死 consumer：未 ack 消息是否重新投递且不会造成重复扣减？
- publish 后断网：confirm 超时是否重发，重复是否可接受？
- routing key 写错：mandatory/AE 是否发现？
- 下游停 30 分钟：队列限制、磁盘、告警与恢复速度是否符合预期？
- poison message：是否按上限进入 DLQ而非死循环？
- broker 重启/节点故障：durable、persistent、quorum、重连是否实际验证？
- schema 加字段/版本升级：新旧生产者消费者能否滚动发布？

---

## 11. 生产运维、安全和可观测性

### 11.1 集群与高可用

RabbitMQ cluster 共享用户、vhost、exchange、binding、queue/stream 定义、policy 等元数据，但“加入 cluster”不等于所有 queue 内容都复制。classic queue 只有一个副本；quorum queue 和 stream 才按各自机制复制内容。

生产通常使用 3 个节点起步，奇数节点便于多数派。客户端经负载均衡或 endpoint 列表连接任意节点；节点故障后客户端仍需重连，broker 不会把一条已断 TCP 连接瞬移到别处。cluster 面向稳定低延迟网络；跨 WAN/跨独立地域优先考虑 Federation 或 Shovel，而非轻率拉伸单集群。

- **Federation**：异步连接独立 broker/cluster。exchange federation 复制发布流；queue federation 主要在上游无本地消费者时按需拉取。
- **Shovel**：像可靠客户端一样从 source consume 并向 destination republish，始终单向搬运；dynamic shovel 可在线配置，通常优先。

二者都要监控 link 状态、配置多个 endpoint/TLS，并理解至少一次可能产生重复。

### 11.2 Policy、Parameter 与 Definitions

- policy：按正则给 queue/exchange 动态应用 TTL、DLX、长度、federation 等配置；有 priority 决定冲突。
- operator policy：平台方限制资源使用，优先级高于应用参数；数值通常取更保守的值。
- runtime parameter：插件/组件的动态配置，例如 federation upstream、dynamic shovel。
- definitions：用户、vhost、权限、topology、policy、parameter 等元数据的 JSON 导入导出；不包含队列消息。

备份必须区分 definitions 和消息数据。节点数据目录快照需遵循官方一致性要求；关键业务恢复能力更多依赖复制、应用重放源、灾备演练，而不是只导出 definitions。

### 11.3 安全清单

- 不在生产使用默认 `guest` 或广泛管理员账号；每服务独立最小权限用户。
- 不把 15672、25672、4369 等管理/集群端口暴露给公网业务侧。
- AMQP、管理 API、集群间连接使用 TLS，验证服务端证书；需要时做 mTLS。
- 密码使用强哈希，凭据轮换；可集成 LDAP、OAuth 2 等认证后端。
- vhost、per-user/per-vhost limits 防止单租户耗尽连接、channel、queue。
- Management UI/API 权限最小化，审计配置变更。
- 消息 payload 分类、脱敏和加密；DLQ、日志、trace 同样受数据治理约束。
- Erlang cookie 是节点/CLI 分布式认证秘密，不是应用 AMQP 密码。

### 11.4 监控什么

生产推荐 Prometheus 插件采集，Grafana 展示，Management UI 用于短期观察和运维，不作为唯一长期监控。

| 指标/现象 | 含义与告警思路 |
| --- | --- |
| messages ready | 等待交付；持续上升说明生产快于消费或 consumer 故障 |
| messages unacknowledged | 已交付未确认；过高可能处理慢、prefetch 大、handler 卡住 |
| publish/deliver/ack/redeliver rate | 判断流量和重复/故障趋势 |
| oldest message age | 比单纯 queue length 更接近用户等待时间/SLO |
| consumer count/capacity | consumer 是否掉线、是否还有消费能力 |
| connection/channel count | 泄漏、连接风暴、错误的每请求建连 |
| memory/disk/file descriptor | 资源水位和 alarm 风险 |
| node/queue leader/replica health | quorum 是否完整、节点是否可用 |
| unroutable/returned、confirm latency | 拓扑错误与 broker/磁盘压力 |
| DLQ depth/retry rate | 业务或依赖异常，不应为静默黑洞 |

监控 broker 和应用两侧：publisher 成功/失败/confirm 延迟，consumer 成功/失败/耗时/重试/幂等命中，端到端 event age 和 correlation trace。

### 11.5 健康检查

- liveness：进程是否活着，避免依赖故障导致反复重启风暴。
- readiness：应用是否能接受流量；producer 是否连通、consumer 是否已订阅可按业务决定。
- broker 使用有针对性的 `rabbitmq-diagnostics` 检查。不要把大量昂贵检查每秒执行；健康检查覆盖越多不一定越好，错误阈值会造成假阳性。

### 11.6 升级和容量

关注 RabbitMQ 与 Erlang 兼容矩阵、版本支持期、弃用特性和 feature flags。升级前导出 definitions、验证备份和回滚/前进方案、在预生产演练；集群滚动升级必须遵守官方允许的版本路径。RabbitMQ 4.0 已移除 classic mirrored queues，4.3 使用 Khepri 作为唯一元数据存储，不能照搬老教程。

容量压测要使用接近真实的消息大小、持久化、confirm、queue type、复制数、consumer 处理耗时和故障场景；只测空消息的峰值 TPS 没有上线意义。磁盘延迟与吞吐通常对持久/复制队列非常关键。

官方：[Production Checklist](https://www.rabbitmq.com/docs/production-checklist)、[Clustering](https://www.rabbitmq.com/docs/clustering)、[Monitoring](https://www.rabbitmq.com/docs/monitoring)、[Prometheus](https://www.rabbitmq.com/docs/prometheus)、[Security/TLS](https://www.rabbitmq.com/docs/ssl)、[Access Control](https://www.rabbitmq.com/docs/access-control)、[Policies](https://www.rabbitmq.com/docs/policies)、[Federation](https://www.rabbitmq.com/docs/federation)、[Shovel](https://www.rabbitmq.com/docs/shovel)。

---

## 12. 常见故障排查手册

### 12.1 能连接但立即 channel closed

先看客户端异常和 broker log。常见原因：同名 queue/exchange 属性不等价（406）、资源名/权限不足（403）、向不存在 exchange 发布（404）、在错误 channel ack。connection 仍可能存在，但发生协议异常的 channel 已关闭，不能继续复用。

### 12.2 消息“发送成功”但 queue 没有

依次检查：

1. 发到了哪个 vhost/exchange？
2. exchange 是否存在、类型是否正确？
3. routing key 与 binding 是否匹配，topic 的 `*`/`#` 是否理解正确？
4. 是否启用 mandatory return 或 AE？
5. publisher 是否真正等待 confirm？
6. 消息是否迅速被 consumer 取走，转为 Unacked 或已 ack？
7. 是否 TTL 到期、超过 max length、进入 DLQ？

### 12.3 Ready 持续增长

consumer count 是否为 0；消费速率是否低于发布；handler 是否被数据库/API 限流；任务能否水平扩容；是否有 poison message 卡住顺序；prefetch 是否过低；CPU/磁盘/网络是否瓶颈。先保护系统和估算清空时间，再扩容/修复，不要直接 purge 生产队列。

### 12.4 Unacked 很高

通常是 handler 慢/卡死、prefetch 过大、忘记 ack、promise 未完成或 channel 共享错误。降低 prefetch 只能限制新增 in-flight，不能自动修复业务阻塞。关闭 consumer connection 会让未确认消息重新入队，可能造成重复和突发流量。

### 12.5 重复消息

这是 at-least-once 的正常可能结果，不先指责 RabbitMQ。检查 producer confirm 超时重发、consumer 提交后 ack 前崩溃、连接恢复、nack/requeue、处理超时。用 messageId、redelivered、x-death、应用日志与 trace 还原；最终修复是幂等和正确事务边界。

### 12.6 消费看起来乱序

检查 consumer 数、prefetch、并行 Promise、priority、redelivery 和 producer channel 数。先确认业务是否真的需要全局严格顺序；通常只需要同一 entity key 的相对顺序。

### 12.7 Broker 阻塞生产者

查看 memory/disk alarm、connection state、blocked 通知和日志。释放根因（恢复磁盘、停止异常生产、加快消费），不能只重启。磁盘低水位是保护数据安全的机制。

### 12.8 排错证据清单

时间范围、vhost、exchange、queue、routing key、messageId、correlationId、连接名、consumer tag、RabbitMQ/客户端版本、日志、Ready/Unacked/速率、最近配置变更。没有这些信息时，“RabbitMQ 丢消息”通常只是猜测。

---

## 13. RabbitMQ、Kafka 与 BullMQ 怎么选

| 维度 | RabbitMQ | Kafka | BullMQ |
| --- | --- | --- | --- |
| 核心抽象 | exchange + queue，消息交付后确认删除 | 分区追加日志 + offset | Redis 上的 job queue |
| 强项 | 灵活路由、任务、低延迟、ack/retry/DLX/RPC | 高吞吐、长保留、历史回放、流处理 | Node/Nest 后台任务、延迟/重复 job、上手快 |
| 多订阅 | 每个订阅者队列或 stream consumer | consumer group 各自 offset | 通常围绕 job worker |
| 顺序 | 单 queue/单 consumer 较直观，扩容后需设计 | 分区内顺序 | queue/job 语义，受并发影响 |
| 回放 | 普通 queue 不适合；stream 支持 | 核心能力 | 已完成 job 可保留但不是事件日志 |
| 运维依赖 | RabbitMQ/Erlang | Kafka 集群 | Redis |

选择问题：是否需要历史回放？保留多久、吞吐和消息大小？路由复杂度？任务还是事件日志？团队已有哪套基础设施？失败/重复/顺序/SLO 如何定义？不要用“谁性能更高”一题决定。

---

## 14. 全栈岗位 RabbitMQ 面试题与参考答案

### 14.1 基础与路由

1. **RabbitMQ 的消息路径？** Producer 通过 connection 上的 channel 向 exchange 发布，exchange 根据类型、routing key 与 binding 路由到一个或多个 queue，consumer 接收 delivery 并 ack/nack。
2. **为什么需要 exchange？** 解耦生产者与具体 queue，用可演进的路由规则支持精确、广播和模式分发。
3. **direct、fanout、topic、headers？** 分别是精确 key、广播、点分段通配、header 条件匹配。
4. **同一 queue 两个 consumer 会都收到吗？** 不会，它们竞争消费；要广播就给每个订阅者独立 queue。
5. **默认 exchange 是什么？** 名为 `""` 的预声明 direct exchange，队列以自身名字自动绑定，因此可 `sendToQueue`。
6. **vhost 有何用？** 隔离资源、policy 和权限的逻辑租户边界；connection 一次连接一个 vhost。
7. **connection 和 channel？** TCP connection 成本高应复用；channel 是其上轻量逻辑会话，协议操作和 ack/confirm 都在 channel 范围。
8. **queue declare 为什么会 406？** 同名队列存在但声明属性/不可变参数不等价。

### 14.2 可靠性

9. **publisher confirm 和 consumer ack 区别？** confirm 覆盖 publisher→broker；ack 覆盖 broker→consumer 的处理结果，彼此独立。
10. **persistent=true 是否绝不丢？** 否；还需 durable topology、正确路由、confirm，关键场景还需 replicated queue、可靠磁盘和应用恢复。
11. **at-least-once 为什么重复？** 业务提交后 ack 丢失/崩溃，或发布成功后 confirm 未到导致 producer 重发。
12. **如何实现幂等？** 稳定 messageId，消费者数据库唯一键，在同一事务中记录已处理并更新业务；外部 API 使用幂等键。
13. **如何解决数据库与发消息一致性？** Transactional Outbox：业务与 outbox 同事务，relay confirm 发布，消费者幂等。
14. **自动 ack 风险？** broker 写入 socket 后就认为成功，consumer 进程在业务完成前崩溃会丢处理机会。
15. **nack requeue=true 有何风险？** 永久错误形成立即重投死循环；应分类错误、延迟且有限重试、最终 DLQ。
16. **mandatory、confirm、DLX、AE 分别发现什么？** mandatory/AE 处理初始不可路由；confirm 表示 broker 对发布的确认；DLX 处理进入 queue 后因拒绝/TTL/超限等死亡的消息。
17. **如何保证严格顺序？** 先缩小为某 key 的顺序；固定分片、单活/单 consumer、版本检查。承认吞吐和可用性代价。
18. **RabbitMQ 支持 exactly-once 吗？** 普通 queue 原生提供的实际常用语义是 at-most/at-least once；业务恰好一次效果靠幂等、事务和状态机实现。

### 14.3 性能、队列与故障

19. **prefetch 做什么？** 限制 consumer 未确认在途消息窗口，平衡吞吐、公平性和内存；不是 producer 限流，也不直接等于 handler 并发数。
20. **Ready 与 Unacked？** Ready 尚未交付；Unacked 已交付等待确认。二者异常指向的排查方向不同。
21. **classic 与 quorum？** classic 内容单副本；quorum 基于 Raft 复制，需多数在线，适合关键持久队列。4.0 已移除 classic mirroring。
22. **stream 与 queue？** queue 通常竞争消费并确认移除；stream 是保留的追加日志，可按 offset 重读，适合回放和大吞吐。
23. **为何推荐奇数节点？** 多数派容错；3/4 节点都只能容忍 1 个失效，偶数常不增加容错能力。
24. **TTL + DLX 如何重试？** 失败消息进入带 TTL 的 retry queue，过期后通过 DLX 路由回 main；记录次数并最终 DLQ。
25. **DLQ 应如何运营？** 指标告警、查看/脱敏工具、归因、修复、幂等的受控重放、审计；不能只建不管。
26. **如何处理积压？** 确认生产/消费速率和瓶颈，保护资源、入口背压、扩容/优化消费者、队列上限与 TTL，估算恢复时间。
27. **publish 返回 false 是 nack 吗？** 不是，是 Node writable buffer 背压信号；应等 drain。broker confirm 是另一套机制。
28. **broker memory/disk alarm？** 为保护节点，RabbitMQ 阻塞发布连接；需监控 blocked 通知和资源水位。

### 14.4 架构、Nest 与运维

29. **Nest `emit` 与 `send`？** emit 是事件、无需业务响应；send 是 request/reply、返回 cold Observable 必须订阅并设置超时。
30. **Nest 如何手动 ack？** `noAck:false`，handler 从 `RmqContext` 获取 channel/message，在业务提交后 `ack`，失败按分类 `nack`。
31. **为什么不在每次 HTTP 请求 connect？** 握手/TLS/资源成本高，会造成连接风暴；应复用长连接和受控 channel。
32. **如何优雅停机？** 停入口/取消消费，等待 in-flight，在超时内 ack，关闭 channel/connection；剩余未 ack 会重投，故仍需幂等。
33. **cluster 是否自动复制所有消息？** 否，元数据共享不等于 classic queue 内容复制；选择 quorum/stream 才有对应复制。
34. **跨机房用 cluster 还是 Federation/Shovel？** cluster 偏稳定低延迟网络；独立地域/WAN 通常用异步 Federation/Shovel降低可用性耦合。
35. **Federation 与 Shovel？** Federation 按 exchange/queue 语义联邦；Shovel 始终单向 consume+republish 搬运，更显式灵活。
36. **生产监控最重要什么？** Ready/Unacked、速率、oldest age、consumer、redelivery、DLQ、连接/channel、confirm latency、资源 alarm、节点/副本健康。
37. **policy 为什么优于硬编码 x-argument？** 可动态、批量修改且不需应用重新部署；不可变参数仍需声明时确定。
38. **definitions 备份包含消息吗？** 不包含，只是用户、vhost、权限、topology、policy 等元数据。
39. **RabbitMQ 和 Kafka？** RabbitMQ 强在队列交付与灵活路由；Kafka 强在分区日志、长期保留和回放；按语义/SLO/团队能力选。
40. **你会如何设计订单异步处理？** Outbox+confirm，topic exchange，每订阅者独立 quorum queue，手动 ack、幂等事务、有限延迟重试、DLQ 运营、契约版本、监控与故障演练。

回答面试题时使用“机制 → 能解决的问题 → 仍然存在的边界 → 项目做法”四段式，比只背 API 更有说服力。

---

## 15. 动手练习与验收标准

### 练习 1：观察竞争消费

启动两个 `rabbitmq-demo` consumer，将 `PREFETCH_COUNT=1`、处理延迟调大，发布 10 条消息。观察消息分配、Ready/Unacked 与关闭一个 consumer 后的变化。

### 练习 2：验证至少一次

在业务输出后、ack 前临时让进程退出。重启 consumer，观察 redelivery。把内存去重改成 SQLite/PostgreSQL 唯一键，证明业务结果不重复。

### 练习 3：无路由消息

发布错误 routing key，先观察默认静默丢弃，再加入 `mandatory:true` 和 `return` 事件，最后配置 AE。解释它与 DLX 的差别。

### 练习 4：故障与 DLQ

启用 `DEMO_FAILURES=true`，观察三次延迟重试和 DLQ。打印 headers/x-death，写一个只允许指定 messageId、带审计日志的重放脚本。

### 练习 5：NestJS 订单

跑通成功事件、RPC、失败死信；再启动两个 Nest 实例（修改 HTTP 端口，共用 queue），观察竞争消费。把 `Set` 替换为数据库去重表。

### 练习 6：生产设计题

为“用户上传视频后转码、生成缩略图、通知”画 topology，说明消息契约、重试、超时、幂等、优先级、积压限制、监控、容量和大文件存储。能在 10 分钟内讲清楚，即达到初级全栈面试可用水平。

### 最终自检

- [ ] 不看资料画出 producer→exchange→queue→consumer→ack。
- [ ] 写出 direct/fanout/topic 的拓扑与适用场景。
- [ ] 独立运行 Node 和 NestJS 两个 Demo。
- [ ] 解释 durable/persistent/confirm/ack/quorum 各自边界。
- [ ] 解释重复消息并实现数据库幂等。
- [ ] 设计有限延迟重试、DLQ 告警和重放。
- [ ] 根据 Ready/Unacked/速率排查积压。
- [ ] 能说清 Outbox、最终一致性和 Saga。
- [ ] 给出 RabbitMQ/Kafka/BullMQ 的选择依据。
- [ ] 完成 40 道面试题的闭卷口述。

---

## 16. RabbitMQ 官方知识全景索引

这里覆盖官方文档的全部知识域，避免把“学习手册完整”误解为只会六个教程。第一次学习按级别掌握；链接默认指向当前稳定版 4.3。

### A. 开发者核心（必须掌握）

- 入门与协议概念：[Tutorials](https://www.rabbitmq.com/tutorials)、[AMQP Concepts](https://www.rabbitmq.com/tutorials/amqp-concepts)、[Protocols](https://www.rabbitmq.com/docs/protocols)。
- 发布与路由：[Publishers](https://www.rabbitmq.com/docs/publishers)、[Exchanges](https://www.rabbitmq.com/docs/exchanges)、[AE](https://www.rabbitmq.com/docs/ae)、[Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to)、[Sender-selected Distribution](https://www.rabbitmq.com/docs/sender-selected)。
- 消费：[Consumers](https://www.rabbitmq.com/docs/consumers)、[Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)、[Consumer Priority](https://www.rabbitmq.com/docs/consumer-priority)、[Consumer Cancellation](https://www.rabbitmq.com/docs/consumer-cancel)、[Nack](https://www.rabbitmq.com/docs/nack)。
- 队列：[Queues](https://www.rabbitmq.com/docs/queues)、[Classic](https://www.rabbitmq.com/docs/classic-queues)、[Quorum](https://www.rabbitmq.com/docs/quorum-queues)、[TTL](https://www.rabbitmq.com/docs/ttl)、[DLX](https://www.rabbitmq.com/docs/dlx)、[Length](https://www.rabbitmq.com/docs/maxlength)、[Priority](https://www.rabbitmq.com/docs/priority)。
- 可靠性：[Reliability](https://www.rabbitmq.com/docs/reliability)、[Confirms/Acks](https://www.rabbitmq.com/docs/confirms)、[Semantics](https://www.rabbitmq.com/docs/semantics)。
- 客户端基础：[Connections](https://www.rabbitmq.com/docs/connections)、[Channels](https://www.rabbitmq.com/docs/channels)、[Heartbeats](https://www.rabbitmq.com/docs/heartbeats)、[URI](https://www.rabbitmq.com/docs/uri-spec)。

### B. 项目进阶（理解并实践）

- 流：[Streams/Super Streams](https://www.rabbitmq.com/docs/streams)、[Stream Protocol](https://www.rabbitmq.com/docs/stream)、[Connections](https://www.rabbitmq.com/docs/stream-connections)、[Filtering](https://www.rabbitmq.com/docs/stream-filtering)、[Effectively-once](https://www.rabbitmq.com/docs/stream-effectively-once-processing)、[Core plugin 对比](https://www.rabbitmq.com/docs/stream-core-plugin-comparison)。
- 资源与背压：[Flow Control](https://www.rabbitmq.com/docs/flow-control)、[Blocked Connections](https://www.rabbitmq.com/docs/connection-blocked)、[Memory](https://www.rabbitmq.com/docs/memory)、[Memory Use](https://www.rabbitmq.com/docs/memory-use)、[Disk Alarms](https://www.rabbitmq.com/docs/disk-alarms)、[Limits](https://www.rabbitmq.com/docs/limits)。
- 拓扑治理：[Policies](https://www.rabbitmq.com/docs/policies)、[Parameters](https://www.rabbitmq.com/docs/parameters)、[Definitions](https://www.rabbitmq.com/docs/definitions)、[E2E](https://www.rabbitmq.com/docs/e2e)、[Validated User ID](https://www.rabbitmq.com/docs/validated-user-id)。
- 可观测性：[Monitoring](https://www.rabbitmq.com/docs/monitoring)、[Prometheus](https://www.rabbitmq.com/docs/prometheus)、[Management](https://www.rabbitmq.com/docs/management)、[HTTP API](https://www.rabbitmq.com/docs/http-api-reference)、[Logging](https://www.rabbitmq.com/docs/logging)、[Firehose](https://www.rabbitmq.com/docs/firehose)、[Event Exchange](https://www.rabbitmq.com/docs/event-exchange)。
- 安全：[Authentication](https://www.rabbitmq.com/docs/authentication)、[Authorization](https://www.rabbitmq.com/docs/access-control)、[Passwords](https://www.rabbitmq.com/docs/passwords)、[TLS](https://www.rabbitmq.com/docs/ssl)、[OAuth 2](https://www.rabbitmq.com/docs/oauth2)、[LDAP](https://www.rabbitmq.com/docs/ldap)、[Vhosts](https://www.rabbitmq.com/docs/vhosts)、[User Limits](https://www.rabbitmq.com/docs/user-limits)。

### C. 平台与分布式（会选型、按需操作）

- 集群：[Clustering](https://www.rabbitmq.com/docs/clustering)、[Formation](https://www.rabbitmq.com/docs/cluster-formation)、[Partitions](https://www.rabbitmq.com/docs/partitions)、[Inter-node TLS](https://www.rabbitmq.com/docs/clustering-ssl)、[Networking](https://www.rabbitmq.com/docs/networking)。
- 跨集群：[Distributed Messaging](https://www.rabbitmq.com/docs/distributed)、[Federation](https://www.rabbitmq.com/docs/federation)、[Federated Exchanges](https://www.rabbitmq.com/docs/federated-exchanges)、[Federated Queues](https://www.rabbitmq.com/docs/federated-queues)、[Shovel](https://www.rabbitmq.com/docs/shovel)、[Dynamic Shovel](https://www.rabbitmq.com/docs/shovel-dynamic)。
- 配置运维：[Configure](https://www.rabbitmq.com/docs/configure)、[CLI](https://www.rabbitmq.com/docs/cli)、[Plugins](https://www.rabbitmq.com/docs/plugins)、[Production Checklist](https://www.rabbitmq.com/docs/production-checklist)、[Backup](https://www.rabbitmq.com/docs/backup)、[Upgrade](https://www.rabbitmq.com/docs/upgrade)、[Feature Flags](https://www.rabbitmq.com/docs/feature-flags)、[Deprecated Features](https://www.rabbitmq.com/docs/deprecated-features)。
- 元数据：[Metadata Store/Khepri](https://www.rabbitmq.com/docs/metadata-store)。RabbitMQ 4.3 以 Khepri 为唯一 metadata store；旧 Mnesia 教程仅作历史参考。
- 部署：[Installing](https://www.rabbitmq.com/docs/download)、[Kubernetes DIY](https://www.rabbitmq.com/docs/install-kubernetes-diy) 与 [Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview)。

### D. 多协议、插件与内部能力（知道用途）

- AMQP 1.0：[AMQP 1.0](https://www.rabbitmq.com/docs/amqp)；AMQP 0-9-1：[Specification](https://www.rabbitmq.com/docs/specification) 与 [Reference](https://www.rabbitmq.com/amqp-0-9-1-reference)。
- 物联网/Web：[MQTT](https://www.rabbitmq.com/docs/mqtt)、[Web MQTT](https://www.rabbitmq.com/docs/web-mqtt)、[STOMP](https://www.rabbitmq.com/docs/stomp)、[Web STOMP](https://www.rabbitmq.com/docs/web-stomp)。浏览器通常经 Web MQTT/Web STOMP/WebSocket 或后端 API，不直接暴露内部 AMQP 凭据。
- 跨协议：[Property Conversions](https://www.rabbitmq.com/docs/conversions)，跨协议消费时属性需要转换并可能有语义差异。
- 扩展：[Plugins](https://www.rabbitmq.com/docs/plugins)、[Exchange Extensions](https://www.rabbitmq.com/docs/extensions)、[Message Interceptors](https://www.rabbitmq.com/docs/message-interceptors)、[Local Random](https://www.rabbitmq.com/docs/local-random-exchange)、[Modulus Hash](https://www.rabbitmq.com/docs/modulus-hash-exchange)。
- 故障支持：[Troubleshooting](https://www.rabbitmq.com/docs/troubleshooting)、[Networking](https://www.rabbitmq.com/docs/troubleshooting-networking)、[TLS](https://www.rabbitmq.com/docs/troubleshooting-ssl)、[Snapshots](https://www.rabbitmq.com/docs/snapshots)、[Signatures](https://www.rabbitmq.com/docs/signatures)。

### E. 新手第一轮可以跳过什么

安装包构建、Erlang runtime 调优、Khepri 故障恢复细节、插件开发、协议 wire-level 规范、OAuth 各身份商配置、EC2 peer discovery 细节、跨大版本蓝绿/滚动升级步骤可以先跳过。你需要知道它们属于哪个知识域和何时查官方文档，但无需为了“完整”在第一次学习中背诵。

---

## 结语：真正需要记住的十句话

1. Producer 通常发布到 exchange，exchange 通过 binding 把消息路由到 queue。
2. 同一 queue 的 consumer 竞争消费；广播要为每个订阅者建立独立 queue。
3. confirm 管发布，ack 管消费，它们互不替代。
4. durable queue + persistent message + confirm 才构成基础持久发布链，关键数据再考虑 quorum。
5. at-least-once 必然允许重复，消费者必须幂等。
6. 数据库与消息一致性用 Outbox，不能靠两行顺序调用侥幸保证。
7. 重试必须延迟、有上限、可观测，永久失败进入有人负责的 DLQ。
8. prefetch 控制未确认窗口；Ready、Unacked 与 oldest age 是排查积压的关键。
9. cluster 不等于所有消息复制；classic、quorum、stream 要按语义选择。
10. 能画拓扑、说清失败边界并跑过故障实验，才算真正会在项目中使用 RabbitMQ。
