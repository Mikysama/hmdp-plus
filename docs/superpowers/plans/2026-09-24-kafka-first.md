# Kafka First Implementation Plan

**Goal:** 将当前同步数据库受理移到 Kafka 消费端，恢复 Caffeine/Redis/Kafka/MySQL 顺序。
**Architecture:** 热入口仅缓存和消息；消费端事务原子受理建单。后置 Outbox 保留通知/Redis 投影；旧 V2 topic 仅排空。
**Spec:** ../specs/2026-09-24-kafka-first-design.md
**Tech Stack:** 现有 Spring Kafka、Caffeine、Redis Lua、MySQL/ShardingSphere；不升级依赖。

- [x] 测试先行：Facade 成功在 Kafka ACK 后返回 QUEUED 且数据库/会员零调用；发送失败保留资格。
- [x] SeckillAdmissionCache / SeckillAdmissionPublisher：Redis-only 缓存加载、固定 V3 事件、有限发送等待、acks=all。
- [x] SeckillQueuedProcessor / Transactions.fulfill：会员读取在消费端、合并本地事务、幂等、期限与旧代次终态；不写 CREATE 事件。
- [x] Kafka listener、查询、孤儿扫描及订阅失败处理接线。默认消费并发 1，max.poll.records 10；入口名额仅约束生产请求。
- [x] 前端 QUEUED/PENDING 与不确定查询保持请求身份；升级集成测试与文档，旧压测数据标为历史架构。
- [x] 执行 Maven 单元/真实 Kafka 测试、Redis 协议、前端测试与构建、两个独立 MySQL 端到端验证；记录实际结果。

运行：scripts/test-seckill-v2.sh；scripts/run-seckill-v2-mysql-tests.sh -Dtest=SeckillShardingIT,SeckillEndToEndIT。
仓库无 HEAD，保留原地工作与备份，不创建伪基线或提交其他未跟踪文件。

验证结果：113 次单元/事务/Kafka 测试执行通过；双 MySQL 集成 9 通过、1 可选压测跳过；Redis 36、迁移 12、前端 11 全通过，前端构建与修改文件 lint 通过。详见 docs/seckill-v2/kafka-first-verification.md。
