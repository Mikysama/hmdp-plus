# Kafka 前置秒杀设计

用户已指定主链路 Caffeine → Redis → Kafka → MySQL，本次原地修改；仓库没有历史提交，不执行破坏性回滚。

- Caffeine 缓存 Redis 活动元数据，1 秒过期、最多 10000 券。缓存缺失只读 Redis，不访问数据库；Lua 最终校验规则与库存。
- Facade 只做本地缓存检查、有界准入、Redis 预扣和 Kafka 发送。会员资料与数据库规则校验移入消费者。
- 独立 seckill-admission-v3 Topic/消费组，schemaVersion=3，携带固定 reservation/order/request/event ID、epoch、创建时间、autoIssue。
- Broker ACK 后返回 QUEUED；发送超时返回 503 DELIVERY_UNCONFIRMED，保留同一资格与请求。消费完成才返回 SUCCEEDED。
- 消费端同券本地事务合并受理和建单，仍写流水和后置 Redis/通知 Outbox，不再产生新建单 CREATE Outbox。
- 数据库旧 CREATE 事件及 V2 消费者只用于升级排空；新请求不经过数据库前置 Outbox。
- 队列等待期限默认 60 秒，从 Redis 预扣时间起算。超过期限的消费持久化失败；孤儿扫描至少等待队列期限再核查，不能因默认 30 秒扫描误取消正常排队。
- 旧代次未落库消息持久化 FAILED，不扣新代次库存；已有记录重放返回原结果。故障重建可能终止尚未落库的排队请求，这是明确边界。
- 查询先读当前用户数据库结果；无记录时可从 Redis 返回 PENDING（尚未确认数据库结果），绝不由 Redis 推断成功。无记录仍返回 NOT_FOUND。
- 订阅补发仍走相同入口，异步失败标记对应订阅轮次；消息重试和 DLT 保留。取消、调整、投影与同券分片继续使用已有保护。
- 测试必须证明请求线程不访问数据库/会员、Kafka 失败不释放库存、消费重放/失败/旧代次安全、前端排队不误判失败；隔离中间件回归，不访问开发业务数据。
