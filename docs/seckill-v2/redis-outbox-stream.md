# Redis admission Outbox Stream

## 投递链路

Redis `reserve.lua` 在同一次脚本执行中写入预占与待投递消息。每张券使用 `${seckill.v2.prefix}:{voucherId}:admission-outbox`，与预占数据使用相同 Redis Cluster hash tag。Stream 不按 epoch 分键，重建不会删除尚未确认的记录。

每条消息包含 `reservation`（原始预占 JSON，含 requestId、orderId、voucherId、userId、epoch、ruleVersion、createdAt、HELD 状态）及 `autoIssue`。预占 JSON 中还固定保存 autoIssue，重复 requestId 只能沿用原模式（否则 AUTO_ISSUE_MISMATCH），返回原预占而不新增 Stream 消息。重建的数据库绑定可以省略该字段，因为数据库已有请求结果决定幂等处理。已删除的投递记录也不会因为同请求重试而重新建一条；HTTP 自身仍可用同身份投递。

HTTP 保留原同步快路径：Kafka ACK 后返回 QUEUED，发送结果不确定时返回 503 DELIVERY_UNCONFIRMED 并保留预占。Stream 后台转发不依赖客户端继续在线或重新 POST。前端仍以查询到的数据库 SUCCEEDED 为成功依据。

## 后台转发与故障

`SeckillAdmissionRelay` 使用独立 `seckillStreamScheduler`，默认 `seckill.v2.stream-relay-ms=500`。按券扫描，每次最多读 100 条 Stream 记录；单轮时间预算 2 秒，已经开始的同步 Kafka 发送可超过该预算，之后不再发起新发送。每轮固定扫描结束位置，读完后回到旧记录，防止持续新流量饿死旧消息重试。它使用 XRANGE + ACK 后 XDEL，而非消费组：消息留在 Stream 本身就是未完成任务，不存在需要额外回收的 PEL。

- Kafka 发送异常或 ACK 超时：保留消息，后续扫描重试。
- 进程退出：新实例从 Redis 中的未删除记录重新扫描。
- Kafka ACK 后、XDEL 前退出或删除失败：可能重投同一消息。数据库的 requestId/orderId、epoch 和终态校验保证业务幂等。
- 多个实例可以扫描同一条记录；同样允许重复投递。HTTP 快路径与 Relay 也可能各投递一次。Kafka producer 幂等不能代替跨实例业务幂等。
- 非法消息：保留并记录告警，扫描越过它继续处理其他记录；需人工核查，不能直接删除未确认消息。
- Redis 重建：旧消息仍会投递，消费者依据数据库决定旧 epoch 或已终止请求的结果，不能根据 Redis 预占丢失直接认定业务失败。

仍保留默认 60 秒排队期限和孤儿预占清理。Kafka 故障超过期限时，后台可能先持久化失败并释放库存；迟到消息不会重新创建订单。Stream 保证后台持续尝试投递，不承诺无限期保留购买资格或必然购买成功。

## 上线与持久性边界

不需要新增 MySQL 表，Kafka schemaVersion 和 Topic 仍为 4 / seckill-admission-v4。沿用当前 Redis 前缀。上线前停止旧版本写入，排空或终结旧 HELD 请求，再部署全部新实例；旧脚本留下的预占没有 Stream 记录，不会自动补建。回退时也必须先排空 Stream，旧版本没有转发器。不得让部分旧实例继续处理新预占。

所有未投递记录禁止设置 TTL、MAXLEN 裁剪或被缓存淘汰。部署应使用持久化 Redis 和 noeviction 策略，并监控内存及 Stream 积压；现有 compose 已开启 AOF，但默认 everysec 仍存在 Redis 主机故障下的刷盘丢失窗口，Stream 不是 MySQL 同等级别的绝对持久保证。Redis 数据整体丢失时依赖数据库重建，尚未进入 Kafka/MySQL 的意图可能丢失。

Lua 提供执行隔离，但不是支持异常回滚的数据库事务：脚本在写之前校验键类型、参数、库存等，XADD 放在其他写操作之前，避免可预见的 Stream 类型/ID 错误留下无投递记录的预占。不要以此宣称 OOM、数据损坏等任意异常都可自动回滚。

## 观测

- `seckill_v2_stream_delivered`：ACK 且 XDEL 返回成功的处理次数。
- `seckill_v2_stream_retry{stage=scan|delivery}`：扫描/发送或删除失败次数。
- Redis XLEN 与最老 Stream entry 的时间：积压和等待时间；不要用 KEYS 扫描生产 Redis。
- `workers-enabled=false` 会同时关闭现有后台任务与 Stream Relay，生产环境需确保开启。

验证记录见同目录 redis-outbox-stream-verification.md。
