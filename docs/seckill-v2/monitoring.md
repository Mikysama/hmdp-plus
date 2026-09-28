# 监控与运行检查

应用端暴露 `/actuator/prometheus`。使用基础设施已有的 MySQL/Redis/Kafka exporter 接入；本改造不部署 exporter。

| 信号 | 来源 / 处置 |
| --- | --- |
| 入口、受理、Redis 预占、恢复耗时 | `seckill_v2_latency_seconds`，按 operation/outcome 标签汇总 P50/P95/P99 |
| 库存守恒或关系异常 | `seckill_v2_audit_failure_total{code="INVENTORY_INVARIANT"}`；立即告警，数据库持久化 PAUSED/INVARIANT |
| Outbox 最老待投递 >30s | 每分钟扫描，`seckill_v2_outbox_overdue_total` + 券 ID 日志 |
| 超过 expires_at 10s 仍 PROCESSING | `seckill_v2_request_overdue_total`，检查消费者和超时任务 |
| Redis 孤儿 HELD >30s | `seckill_v2_orphan_overdue_total` + 最老年龄日志 |
| 投影缺序号 | `seckill_v2_projection_gap_total`；检查前驱 Outbox，不能跳号 |
| 发布/释放/补发重试 | `seckill_v2_outbox_retry_total{type="CREATE/REDIS/REFILL"}` |
| 投影不可达 | `seckill_v2_projection_unavailable_total`，准入 fail closed |
| 扫描或恢复失败 | `seckill_v2_worker_failure_total` + recovery 表 phase/last_error/lease |
| 大券审计遇到并发 | AUDIT_CONCURRENT_RETRY，分页扫描版本变化时重试，不当成一致 |
| 数据库锁/连接池等待 | MySQL performance_schema + Hikari metrics；不以HTTP延迟代替锁等待 |
| Kafka lag / DLT量 | Kafka exporter 的 group `seckill-order-v2` 和 topic `seckill-order-v2.DLT` |
| Redis CPU/Lua耗时 | Redis INFO CPU / commandstats / latency histogram |

默认 HTTP `/actuator` 暴露有限端点。生产环境仍应通过网络和已有认证保护监控端口。

每分钟逐券分页检查，单页100。数据库不变量异常先持久化暂停，再抛出错误，避免事务回滚撤销保护状态。恢复阶段与租约存数据库；线程退出不会丢失任务。

外部告警系统至少配置：库存异常立即通知；Outbox >30s；请求过期10s；恢复长时间停留；DLT新增；单券补发重复失败。日志包含业务券 ID，不包含准入凭证原文。

## Kafka 前置版本

- 新建单积压观察 `seckill-admission-v4` / 同名消费组；DLT 为 `seckill-admission-v4.DLT`。旧 V2 topic 仅用于排空。
- 入口 `QUEUED` 延迟测的是缓存/Redis/生产者 ACK；完成延迟必须另统计消费事务与排队时间。
- 关注 DELIVERY_UNCONFIRMED、QUEUE_EXPIRED、STALE_EPOCH 和 HELD 最老年龄；队列期限默认 60 秒，持续积压应降低入口速率或调整容量与期限，不能把 Kafka ACK 计为成功订单。
- 物理库锁等待/连接池等待仍要监控；默认 consumer-concurrency=1（每实例），多实例/分区并发需合并计算。后置投影与查询/恢复也消耗数据库容量。
