# Kafka 前置验证记录 — 2026-09-24

当前热入口顺序：Caffeine → Redis → Kafka → MySQL。没有运行开发 Spring Boot 配置，没有连接业务 MySQL/Redis/Kafka。

## 已执行

- 后端 `mvn -pl hmdp-core-service -am -DskipTests package` 打包通过。
- Maven `InventoryTest,Seckill*Test`：113 次测试执行，0 failures、0 errors。包含继承的事务夹具，不能当作 113 个互不重复的业务场景。
- 实际随机端口 Kafka KRaft 测试：消息显式身份序列化；数据库提交后 ACK 前故障重投递只建一单；首次加 5 次重试到 DLT；DLT 发送失败不提交恢复 offset；发送失败返回 DELIVERY_UNCONFIRMED；broker ACK 前 publisher 不返回。
- Facade 测试：Caffeine、Redis、publisher 顺序；成功和拒绝路径都不访问会员与数据库；不确定发送重用身份，不释放业务库存；仅数据库终态可报告成功。
- 事务测试：同一事务受理建单、不生成 CREATE Outbox；建单失败连同受理及事件回滚；超时与旧代次消息失败但不扣新库存；并发消费与孤儿终止唯一结果；自动补发失败继续下一候选人。
- Redis 协议：36 项通过；迁移脚本：12 项通过。
- 前端：11 项通过，生产构建通过；本次修改文件 ESLint 通过。QUEUED 在 NOT_FOUND 后保留，刷新仍复用原请求。

## 双物理 MySQL + Redis + Kafka

运行 `bash scripts/run-seckill-v2-mysql-tests.sh -Dmaven.repo.local=.m2/repository -Dtest=SeckillShardingIT,SeckillEndToEndIT`：BUILD SUCCESS，10 项中 9 项通过、1 项可选负载测试未启用，0 failures、0 errors。

使用脚本新建的两个独立 MySQL 实例、随机端口 Redis 和 KRaft，结束自动关闭。验证覆盖：

- 停止消费者后提交得到 QUEUED；数据库结果 NOT_FOUND，库存未预占；查询 PENDING；恢复消费后最终 SUCCEEDED。
- 完整取消/重购、重放旧事件、新代次重建后的库存与购买关系。
- 响应丢失重试保持同一订单；已落库状态在 Redis 清空后仍保留。
- 孤儿终止阻止迟到受理；默认数据库请求创建期限校验。
- 两个独立物理库上的同券事务路由和库存一致性。

## 验证边界

单机中间件测试不等于生产高可用或容量认证。历史 facade-load CSV 是数据库前置版本，当前架构不复用其吞吐结论。新可选 queue-filter 测试在消费者停止时发请求，验证零同步数据库写入；消费者启动后应另测最终完成率、队列期限、数据库压力。

Java 测试仍输出项目既有的多 SLF4J provider 与 Mockito 动态 agent 警告，不影响本次测试结果。
