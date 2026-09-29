# 新秒杀链路布隆过滤器 — 2026-09-29

## 接入位置

复用既有 `BloomFilterHandlerFactory` 的 `voucher` 过滤器（配置 `bloom-filter.filters.voucher`，expected-insertions=100000、false-probability=0.01），不是为订单用户新建过滤器。

- 获取秒杀 token：登录及限流后，`SeckillFacade.issueToken()` 检查过滤器，再生成 token。
- 提交：保留本地并发控制；Caffeine 活动缓存未命中时，先检查过滤器，再加载 Redis 活动元数据。缓存命中不额外访问过滤器。
- Redis Lua、Kafka、数据库资格/库存校验保持不变；命中过滤器不是活动存在或可购买的充分条件。误判为存在的券仍可能获得 token；实际活动有效性在提交阶段检查，不能声称 token 只会发给真实活动。
- 请求结果、已购订单和取消接口不经过布隆过滤器，避免影响数据库已持久化业务结果的查询和处理。

## 负结果和异常处理

| 条件 | 行为 |
| --- | --- |
| Bloom 返回可能存在 | 继续原有 Redis 活动和 Lua 校验 |
| Bloom 返回不存在，但当前准入 `:active` 指针存在 | 继续原校验，尽力补登记，防止过滤器单独丢失/漏加载误拒绝有效活动 |
| Bloom 返回不存在，准入指针也不存在 | 返回 409 `VOUCHER_UNAVAILABLE`，不加载活动元数据、不预占、不写 Stream、不查业务数据库 |
| Bloom 检查抛异常（配置缺失、Redis 故障等） | 记录 fallback 指标，回到原 Redis 准入路径；原校验仍拒绝数据缺失/不可用，不自动回源 SQL |

`VOUCHER_UNAVAILABLE` 不声称数据库中绝对不存在该券。Bloom 与活动数据都被删除时，合法券也可能在后台恢复前暂不可用。

安全代价：负结果仍需一次轻量 Redis 指针探测。此过滤器由 Redisson 存在 Redis 中，不是无网络成本的本地过滤器；不能宣称每个无效 ID 都比旧路径少一次 Redis 请求。主要提供显式存在性过滤、减少无效 token 创建，以及在 Bloom 正常时提前结束后续流程。负结果不缓存为永久不存在。

## 创建、初始化和恢复

- 沿用 `BloomFilterDataInit` 的启动批量加载。
- `SeckillCatalogService.addSeckill()` 校验参数后，在 SQL 事务之前登记新券 ID；登记失败返回 503 `BLOOM_UNAVAILABLE`，尚未写入业务行。
- 登记后 SQL 失败可能留下过滤器假阳性。不得删除该券对应位，因为可能与其他 ID 共用位；后续真实 Redis 和数据库校验仍然有效。
- `SeckillRecovery` 从数据库验证快照后、在重建/激活 Redis 前补登记；网络 IO 不放在数据库事务内。登记失败保存恢复 RETRY 状态，不开放活动。
- `BloomFilterHandler.ensureInitializedAndAdd()` 先 tryInit 再 add，支持 Redis 配置丢失后的恢复，不清空现有过滤器。

不改变已有 MySQL 表、消息版本或 Redis 准入命名空间，无额外 DDL。原有一人一单、库存约束、Outbox 幂等与 epoch 隔离仍是最终业务保护。

## 指标

- `seckill_v2_bloom_rejected`：负结果且无活动指针的拒绝次数。
- `seckill_v2_bloom_fallback{reason=unavailable|existing_admission}`：检查异常降级，或已有活动绕过负结果。
- `seckill_v2_bloom_registration_failed`：创建/恢复/补登记失败。

过滤器接近预计容量或假阳性明显增多时，应规划重建和容量调整；本次不实现自动扩容或在线过滤器切换。

## 验证结果

2026-09-29 将仅含布隆过滤器改动的待提交版本导出至独立目录，完成以下验证，仅使用临时服务，没有连接或修改业务数据库：

```bash
bash scripts/run-seckill-v2-mysql-tests.sh -Dmaven.repo.local=.m2/repository \
  '-Dtest=InventoryTest,Seckill*Test,SeckillShardingIT,SeckillEndToEndIT'
python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
```

- 待提交版本 Java 回归 161 项：160 通过、1 个可选负载测试跳过，0 失败/错误，BUILD SUCCESS。包括双物理 MySQL 分片、真实 Redis/Redisson Bloom、Kafka KRaft 端到端测试。
- Redis Lua 协议 44 项通过；离线迁移 12 项通过。两个临时 MySQL 实例的已有升级校验也通过。
- 布隆专用测试覆盖负结果拒绝、假阳性继续真实校验、位图丢失与补登记、配置丢失降级及重新初始化、活动结束后的原请求重试；创建/恢复登记失败及恢复重试；结果查询不依赖过滤器；Caffeine 热命中不重复查过滤器。
- 端到端测试使用真实布隆过滤器接入创建/恢复和提交路径，保留 Stream 补投、幂等建单、超时及取消恢复回归。
- 独立代码复核未发现阻塞问题。待提交版本验证日志 `/tmp/seckill-bloom-staged-tests.log`，任务开始前快照 `/tmp/hmdp-before-bloom-20260929.tar.gz`。此前包含工作区原有验收及安全改动的完整回归为 169 项（168 通过、1 跳过），日志 `/tmp/seckill-bloom-full.log`。

本次未运行生产容量压测，不据此宣称入口吞吐提升。提交仅包含布隆过滤器接入、相关测试和文档；工作区原有验收和安全改动保留，未部署生产环境。
