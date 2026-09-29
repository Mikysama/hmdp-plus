# 券详情数据库查询布隆过滤器 — 2026-09-29

## 位置与目的

布隆过滤器保护 `POST /voucher/get → SeckillCatalogService.get()` 的 MySQL 查询。数据库是券信息的权威来源，Redis 没有活动准入数据不代表券不存在。

当前详情接口没有详情缓存，实际流程为：

```text
券详情请求 → 检查数据库券 ID 的 Bloom 索引
              ├─ 完整可用且为负 → 404 VOUCHER_NOT_FOUND，不执行详情 SQL
              ├─ 可能存在 → MySQL 查询完整信息，数据库决定是否存在
              └─ 未就绪或异常 → MySQL 查询完整信息
```

若以后增加详情缓存，过滤器应放在缓存未命中后的 SQL 回源之前。本次不引入详情缓存，也不增加秒杀 HTTP 提交的同步 SQL 回源。

- 秒杀提交：Caffeine → Redis 活动元数据/预占 Lua → Kafka，不再检查 Bloom。
- 获取秒杀 token：不再检查 Bloom；令牌不是活动存在或购买成功的证明。
- 活动恢复：不依赖 Bloom 登记，不因目录索引不可用而阻止活动重建。
- 订单结果、取消、Kafka 消费事务保持数据库业务校验，不用 Bloom 负结果覆盖已有订单。
- 不对所有 SQL 统一套用此检查。按商铺查券列表的参数是 shopId，不能用券 ID Bloom 过滤；写入、事务校验等也不受此详情优化替代。

## 实现与初始化

复用 `BloomFilterHandlerFactory` 的 `voucher` 过滤器，底层为 Redisson `RBloomFilter`，数据存在 Redis。它只表示 ID 的集合，不保存完整券信息。

`BloomFilterDataInit` 委托 `SeckillVoucherBloom.initialize()` 通过 `SeckillStore.catalogVoucherIds()` 对 `tb_voucher` 按 ID 游标分批读取（每批最多 1000），涵盖普通券、秒杀券及未激活的券，而不是只加载 Redis 中已开放的活动。初始化是追加式，不清空现有位图，以免与并发创建冲突。券表的 INLINE 分表规则开启范围查询，支持跨两个数据库、四个物理表的排序分页扫描。

`BloomFilterHandler` 为数据库完整加载维护共享的 `catalog-load` 状态，与位图及 Redisson config 使用相同 Redis Cluster hash slot：

1. 初始化位图后写入唯一 `LOADING:<generation>`。
2. 逐批加载数据库 ID；普通券和秒杀券的新建路径均在 SQL 事务之前登记 ID。
3. 仅完整扫描成功、位图及 config 仍存在、generation 未变化时，CAS 更新为 `READY:<generation>`。
4. 券详情仅在 READY 下采用负结果，并在负结果后重新检查 generation；初始化或丢失期间放行 SQL。

位图/config 缺失会撤销加载状态。单条新增登记、补配置不能将不完整索引变成 READY；必须重新完成数据库 ID 扫描。扫描失败、并发加载代次被替换、扫描中观察到数据丢失，都不能发布旧 READY。空目录使用非券 ID 的内部种子保证位图存在。

## 错误与一致性边界

| 情况 | 行为 |
| --- | --- |
| MySQL 有券，Redis 无活动准入指针或完整详情 | 完整索引命中后查 SQL，详情可正常返回 |
| 完整索引负结果 | SQL 前返回 404 VOUCHER_NOT_FOUND |
| Bloom 假阳性或 SQL 写入回滚留下的 ID | 继续查 SQL，由数据库判断，不虚构券信息 |
| Redis/Bloom 异常、加载未完成、加载代次改变 | 放行详情 SQL，记录 fallback |
| 新券登记失败 | 503 BLOOM_UNAVAILABLE，尚未写入券的业务行 |
| 索引丢失后仅登记部分 ID | 保持未就绪，完整数据库重载前不拒绝负结果 |

本次不增加后台自动重建任务。故障排除后重启实例触发全量 ID 重载；重载完成前，详情走 SQL。应在入口保留现有限流，并监控降级期间的数据库压力。

所有新券必须经由登记后提交的写入路径。直接 SQL 导入、旧版本绕过登记写入、外部工具对位图做部分覆盖等不满足此前提；这类操作必须先停用负结果过滤，并重新全量加载。部署要求 noeviction，避免写入期间单独淘汰位图而留下旧加载状态。存在性探测与 Redisson add 并非同一个原子操作，不支持并发写入期间外部删除/重建部分键；此类操作必须先停写并撤销加载状态。不得对位图/config/加载状态进行独立恢复或设置独立过期；恢复与运维操作应按整套索引处理。Redis 的持久化、复制及故障丢失窗口仍存在，不能把 Bloom 当作数据库一致性约束。

数据库可能包含比预计更多的 ID，应监控过滤器容量并按需规划重建；本次不实现自动扩容或在线替换。Bloom 检查本身访问 Redis，只能减少无效详情请求的 SQL，不能据此宣称 Redis 请求次数减少或秒杀吞吐提升。

## 指标

- `seckill_v2_bloom_rejected`：详情查询在 SQL 前被拒绝的次数。
- `seckill_v2_bloom_fallback{reason=unavailable|not_ready}`：异常或未就绪时允许查询 SQL。
- `seckill_v2_bloom_initialization_failed`：全量初始化未完成，包括代次被替换。
- `seckill_v2_bloom_registration_failed`：新建券的登记失败。

## 验证

回归覆盖数据库有券但无 Redis 准入数据、负结果不执行详情 SQL、假阳性回源、初始化失败、部分登记、位图/config 丢失、重载代次隔离、新建登记以及秒杀预占/Stream 不依赖 Bloom。真实 Redis/Redisson 与 SQL 测试验证加载状态，不只模拟接口返回值。

```bash
bash scripts/run-seckill-v2-mysql-tests.sh -Dmaven.repo.local=.m2/repository \
  '-Dtest=InventoryTest,Seckill*Test,UploadSecurityTest,SeckillShardingIT,SeckillEndToEndIT'
python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
```

测试使用临时 Redis、MySQL 和 Kafka 服务，不连接业务环境。不以功能回归代替生产容量压测。

2026-09-29 本次工作区验证：Java 180 项（179 通过、1 个可选负载测试跳过），0 失败/错误；Redis Lua 44 项通过；离线迁移 12 项通过。包含真实双 MySQL 分片的目录分页、Bloom 完整初始化、缺少 Redis 准入数据的详情查询，以及原有 Kafka/Stream 和订单生命周期回归。Java 日志 `/tmp/bloom-db-full.log`。此数字包含工作区已有的验收/安全测试，不是仅本次新增测试数。
