# 秒杀实现与验收：Kafka 前置（2026-09-24）

当前主链路为 **Caffeine → Redis → Kafka → MySQL**。Caffeine 活动缓存只从 Redis 加载；缓存未命中时先检查优惠券布隆过滤器，获取 token 时也检查，Redis Lua 原子预扣资格；HTTP 请求不读取会员、不查询或写入业务数据库，等待 Kafka broker 确认后返回 `QUEUED`。Kafka 消费端读取会员资料，再在同券本地事务中完成资格校验、库存预占、建单和转成交。数据库提交后才有 `SUCCEEDED`。

新消息使用 `seckill-admission-v4`，schemaVersion=4，固定 eventId 与完整 reservation 身份，消息 key=voucherId。启用 acks=all、幂等生产者，业务幂等依赖数据库。发送超时返回 `503 DELIVERY_UNCONFIRMED`，不返还资格，Redis Stream 后台自动补投，客户端查询/重试仍复用原 requestId；仅 Kafka ACK 不是购买成功保证。

后置 Outbox 继续负责 Redis 投影、补发、通知和统计。新链路不产生 CREATE Outbox；旧 `seckill-order-v2` listener 与 CREATE 投递器保留用于升级排空，不能把新消息发进旧 Topic。

消费默认单实例 1 线程、单次 poll 最多 10 条，线程内串行执行数据库事务；多实例有效消费并发仍受分区数和实例配置影响，需要结合数据库容量设置。Kafka 缓冲突发，不代表数据库容量无限。

预占身份统一使用 `orderId`，不再保存独立预占编号；默认 Redis 前缀为 `hmdp:v3`。已有环境必须按 [身份合并升级说明](order-identity-upgrade.md) 停写排空、删除冗余列并重建，不能与旧协议混用。

## 代码入口

| 模块 | 责任 |
| --- | --- |
| `SeckillFacade` | Caffeine → Redis → Kafka 热入口、有界生产并发；独立查询和订阅候选人处理 |
| `SeckillAdmissionCache/Publisher` | Redis-only Caffeine 加载、等待 Kafka ACK、保留发送不确定性 |
| `SeckillVoucherBloom` | 券 ID 存在性过滤、负结果活动指针保护、新建及恢复登记 |
| `SeckillQueuedProcessor` | 消费端会员快照，调用原子受理建单 |
| `SeckillTransactions` | 受理、建单、终止、取消、调配额、订阅；仅同券 SQL |
| `SeckillRecovery` | 持久化冻结/代次/租约/快照/激活、数据库与投影对账 |
| `RedisAdmissionGateway`、`lua/v2/` | 凭证、资格预扣、归属、序号、fence、恢复投影、审计、统计 |
| `SeckillAdmissionRelay` | Redis Stream 持久投递意图扫描，Kafka ACK 后删除，失败自动重试 |
| `SeckillWorkers` | 租约、恢复、超时、Outbox 投递、Redis 投影、补发、告警 |
| `SeckillKafkaConfiguration` | V4 排队与 V2 排空的独立 Topic/组、5 次重试、DLT |
| `SeckillCatalogService` | 完整券模型、规则校验、版本化维护和开抢提醒 |
| `SeckillSecurityInterceptor` | 默认空管理员名单拒绝、管理接口保护 |

### 持久化协议

- `init_stock = stock + reserved_stock + sold_stock`；流水记录每次库存变化前后差额和结果。
- 所有写事务先锁 `tb_seckill_voucher`，核心表按 `voucherId % 2` 分库、`voucherId.intdiv(2) % 2` 分表；绑定表覆盖基础券、库存、请求、订单、资格及事件。范围扫描显式允许 INLINE 全路由。
- 取消匹配订单和正常状态，释放资格匹配 `order_id`。旧取消不能删除新订单关系。
- Redis 无 TTL 业务释放。队列期限 queue-seconds 默认 60 秒，从 Redis 预扣创建时间起算；HELD 达到 max(orphan-seconds, queue-seconds) 后进入核查。没有数据库记录则写失败墓碑及唯一释放事件，迟到消费不能复活；正常消费也校验队列期限。发送/提交未知保留预占。
- 旧代次消息不能修改新投影。每券投影序号严格连续；快照以数据库 `projection_seq` 为水位。
- 重建各阶段持久化，租约失效后新执行者递增 epoch 再建快照。Redis 内单调 fence 防止旧 staging/activate 覆盖更新代次。
- DB OPEN 与 Redis activate 跨系统不原子：间隙通过数据库 epoch 校验保护有效受理，不宣称 Redis 获得资格就是受理承诺。
- Redis 纯不可达时不先冻结正常 DB 券，已受理订单仍可完成；重建中断时保持冻结并重试。
- 缩库存、规则修改进入维护重建；已持久化请求保留原资格；仅 Kafka 排队而未落库的旧代次请求终止为 FAILED/STALE_EPOCH，不会占用新代次库存。
- Kafka 和 Redis/其他事件使用分别有界的投递轮次。准入租约独立调度线程，不会被 Kafka 发送阻塞。

## 接口

所有 ID 对外按字符串处理，业务请求必须保存并复用 `requestId`。

| 接口 | 参数 / 结果 |
| --- | --- |
| GET `/voucher-order/seckill/token/{voucherId}` | 返回短期 token；不传 URL 凭证 |
| POST `/voucher-order/seckill/{voucherId}` | JSON `{requestId,accessToken}` → `{requestId,orderId,status,reasonCode,expiresAt}` |
| GET `/voucher-order/seckill/result` | `voucherId,requestId`；仅当前用户：数据库终态优先；仅有 Redis 预占返回 `PENDING`；两处均无记录为 `NOT_FOUND` |
| POST `/voucher-order/cancel` | JSON `{voucherId,orderId}`；返回 `CANCELLED` 或原结果 |
| POST `/voucher/update/seckill/stock` | `{voucherId,initStock,adjustmentId,expectedVersion}` |
| POST `/voucher/update/seckill` | `{voucherId,expectedVersion,...ruleFields}` |
| GET `/voucher-order/notifications` | `voucherId,after`；当前用户按 event_id 分页的站内记录 |

沿用项目 `Result<T>` 包装，错误包含 `code`。401 未登录，403 管理权限不足，409 确定业务冲突，429 限流，503 依赖不可用或维护。`NOT_FOUND` 与网络异常都不能解释成永久失败。

前端将 `QUEUED`、`PENDING`、旧 `PROCESSING` 都视为待完成；每秒查询，15 秒后保留“仍在处理中”；登录用户和券维度保存原请求以恢复。取消后明确新购买才生成新 ID。旧路由表仅保留迁移备份，新控制器不查询或写路由表。

## 配置

`application.yml` 给出开发起始值：每券成功资格速率 100/s、每券入口发送并发上限 32、单实例入口并发 128、事务超时配置 10s、队列期限 60s、消费者并发 1；这些入口名额不限制 Kafka 积压总数。达到上限立即拒绝，不启动无界等待队列。`seckill.security.admin-user-ids` 默认空，部署时从 `SECKILL_ADMIN_USER_IDS` 设置逗号分隔用户 ID。

`rate-limit.trusted-proxies` 默认空，使用连接来源 IP；配置可信代理后从 X-Forwarded-For 右侧向左跳过已信任代理。入口、凭证和结果查询限流相互区分。不要把所有客户端地址加入白名单。

活动 DATETIME 的解释由 `seckill.v2.activity-zone` 指定，默认 `Asia/Shanghai` 与既有产品一致。MySQL JDBC session 也使用同一时区。测试实例显式 UTC。迁移旧 DATETIME 时保留源语义；不能只改变 JVM 时区。

## 测试安全

旧的两个会连接开发环境的 SpringBoot 测试已禁用。V2 单元/事务测试自行构造 H2；Kafka 测试使用随机端口 KRaft；Redis 脚本测试启动临时 Redis；MySQL IT 必须提供两个独立实例 URL，未提供不会回落到应用数据库。`application-seckill-test.yml` 未配置的 Redis/Kafka 指向不可用端口 1。

```bash
mvn -Dmaven.repo.local=.m2/repository -pl hmdp-core-service -am \
  -Dtest='InventoryTest,Seckill*Test' -Dsurefire.failIfNoSpecifiedTests=false test
python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
cd hmdp-vue3 && npm test && npm run build
```

双 MySQL 路由和真实链路工具见 `sql/v2/README.md`、`scripts/run-seckill-v2-mysql-tests.sh`。完整 IT 使用同样的显式测试 URL 运行 `SeckillEndToEndIT`；会启动自己的 Redis 和 KRaft 并在结束时关闭。

## 停机切换

遵循 `sql/v2/README.md`：备份→关闭旧写入与消费者→只读导出→离线校验并生成全新目标库 SQL→核对明细→同步切换前后端与 V2 配置→恢复投影→冒烟→开放。

脚本不自动操作业务库、不自动部署、不静默修正库存。发现重复正常订单或库存不守恒时阻止生成。旧 Redis/MQ 未落库意图生成迁移终止审计。开放写入后禁止旧备份直接覆盖新订单。

## 运行边界

- 通知表提供按事件去重站内记录；没有配置真实短信或推送提供方，不能把日志当外部通知送达。
- Redis 买家统计为 V2 独立投影，每事件去重，取消终态覆盖迟到成功；原购买日期来自订单创建时间。
- 业务历史/幂等记录本轮不清理；旧 Redis epoch 留存，应在活动终结和恢复完成后另行安排有审计的归档。
- 单券数据库锁仍是有效订单吞吐边界；统计的 Redis 脚本吞吐不是应用 QPS 承诺。
- 自动对账不覆盖篡改后的数据库库存；异常进入 PAUSED/INVARIANT，需要运营检查。

提醒使用 Outbox `next_attempt_at` 持久化调度，到期后分页生成幂等站内记录；不把 Redis 延时队列入队成功作为可靠交付边界。遗留延迟消费者仍能去重处理已有提醒，但新链路不依赖其数据存活。

## 从数据库前置版本切换

1. 停止抢购/补发新写入，排空原 V2 PROCESSING 请求及 CREATE Outbox；保留旧消费者排空能力。
2. 创建独立 V4 Topic 和 DLT，配置 ACL；同时部署新前后端，确认请求返回 QUEUED。
3. 无需回退 V2 同券表结构、库存维度、流水和唯一约束。Redis 继续沿用可恢复的 V2 资格协议，不能清空后按总库存随意回填。
4. 冒烟验证 Kafka 停止消费时库存不在 MySQL 预扣；恢复后请求最终成功，取消只释放一次。
5. 旧 facade-load CSV 属于数据库前置版本的历史测试，不能用于宣称新链路容量。新 queue-filter 测试可用 `-Dseckill.test.load=true` 启用，仅验证停消费者下的入口筛选，不等于端到端容量认证。

Redis 预占同时写入 Outbox Stream，后台在 Kafka ACK 后删除消息；详见 [投递机制与上线约束](redis-outbox-stream.md)。

新链路布隆过滤器的位置、降级边界与初始化策略见 [布隆准入说明](bloom-admission.md)。
