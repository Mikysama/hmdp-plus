# 秒杀订单故障处置

秒杀 Lua 在同一次 Redis 原子操作中扣预订库存、记录 `RESERVED` 状态，并写入
`seckill:order:outbox:{voucherId}` Stream。后台 relay 仅在 Kafka 确认后删除 Stream
记录。Kafka 投递失败时保留记录并继续重试；因此不要因为一次发送失败就补库存。

部署前先运行 `sql/migration/apply_voucher_order_idempotency.sh`。脚本会检查四张
物理订单表是否有重复的正常订单，并跳过已安装的唯一索引。运行时通过
`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER` 和 `MYSQL_PWD` 指定数据库连接。
MySQL 与 Redis 的持久化、备份以及 Atomikos XA 恢复日志都应使用持久化存储。

DLQ 消费者把每个失败订单写入 `tb_rollback_failure_log`，其中
`source = 'seckill_order_dlq'`，并增加 `seckill_order_dlq_total` 指标。收到告警后：

1. 以 `orderId` 查询该日志、Kafka 的 `<prefix>-seckill_voucher_topic.DLQ`、
   对应数据库订单，以及 Redis 的 `seckill:order:state:{voucherId}` 状态。
2. 若数据库订单已存在，按订单结果核对库存、路由及对账日志。不要重新扣库存。
3. 若状态为 `RESERVED` 且无数据库订单，检查对应 Redis outbox Stream；relay
   会持续投递。若 outbox 缺失，应先查明原因并人工重建事件，避免盲目重放。
4. 若状态为 `ROLLED_BACK`，预订库存已经恢复。不得直接重放旧消息；用户需要
   在活动和库存允许时发起新的秒杀请求。
5. 若状态缺失或与数据库冲突，停止自动处理，按流水核对后人工修正。

Redis outbox 的状态 Hash 保留到活动结束后至少七天。若服务或 Kafka 停机超过
这个窗口，relay 会保留状态缺失的 Stream 记录并记录错误，供人工核查。
