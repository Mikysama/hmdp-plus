# HMDP-Plus 后端知识体系与深度学习清单

> 本文以当前 `hmdp-plus` 代码库为准，用于源码复盘和后端面试准备。每个主题应依次达到：**理解原理 → 能沿调用链读代码 → 能独立实现 → 能解释故障与权衡**。

## 项目总览

### 技术栈

- Java 17、Maven 多模块、Spring Boot 3.5.4。
- Spring MVC、Jakarta Validation、Spring AOP、Spring Transaction。
- MyBatis-Plus、MySQL、HikariCP、ShardingSphere-JDBC。
- Redis、Spring Data Redis、Lua、Redisson、Caffeine。
- Kafka、Spring Kafka、手动提交 Offset。
- Micrometer、Actuator、Prometheus、Log4j2。

### 模块职责

| 模块 | 职责 | 学习重点 |
| --- | --- | --- |
| `hmdp-core-service` | Controller、Service、Mapper、Kafka 业务类、Lua 调用 | 完整业务调用链 |
| `hmdp-common` | 常量、枚举、异常、上下文工具 | 公共抽象 |
| `hmdp-parameter` | DTO、VO、参数校验 | 接口契约 |
| `hmdp-redis-tool-framework` | Redis 封装、Key 管理、限流、抢购令牌 | Redis 工程化 |
| `hmdp-redisson-framework` | 分布式锁、防重复、布隆过滤器、延迟队列 | AOP 与组件设计 |
| `hmdp-mq-framework` | Kafka 消息模型、生产者与消费者模板 | 可靠消息 |
| `hmdp-id-generator-framework` | 雪花 ID | 分布式唯一 ID |
| `hmdp-sharding` | ShardingSphere 兼容模块 | 分库分表运行基础 |

### 五条核心调用链

1. 登录：`UserController → UserServiceImpl → MySQL/Redis → Interceptor → UserHolder`。
2. 商户查询：`ShopController → ShopServiceImpl → Caffeine → BloomFilter → Redis → DB`。
3. 社交 Feed：`BlogController → BlogServiceImpl → FollowServiceImpl → Redis ZSet/Set → DB`。
4. 秒杀下单：`VoucherOrderController → 限流/Token → Lua → Kafka → MySQL 分片订单`。
5. 一致性闭环：`Redis 流水 → Kafka 回调 → 对账日志 → 回滚/重建 → 对账任务`。

---

## Java、Spring Boot、AOP、事务基础

### Java 基础

- [ ] 泛型：`Result<T>`、`MessageExtend<T>`、Redis 泛型反序列化。
- [ ] 集合与 Stream：结果组装、ID 收集、关注关系、消息 Header。
- [ ] 时间 API：活动有效期、消息延迟、延迟任务时间计算。
- [ ] 位运算：BitMap 连续签到、雪花 ID 位布局。
- [ ] 异常体系：运行时异常、自定义业务异常、异常传播。
- [ ] 并发基础：可见性、原子性、竞态条件、临界区、锁粒度。
- [ ] `AtomicInteger`、`LongAdder`：线程编号与低竞争计数。
- [ ] `ThreadLocal`：登录上下文、线程复用与内存泄漏。
- [ ] JVM：堆、栈、元空间、GC、线程栈、OOM 分类。

### Spring Boot 与接口层

- [ ] 启动类、组件扫描、Bean 生命周期和依赖注入。
- [ ] `@PostConstruct`：布隆数据、缓存清理、Lua 初始化。
- [ ] `@PreDestroy`：线程池优雅关闭。
- [ ] `@ConfigurationProperties`：布隆、限流、Redisson、延迟队列配置。
- [ ] `AutoConfiguration.imports` 与自定义 Starter/自动配置。
- [ ] Spring MVC 映射、请求参数、JSON 请求体、Multipart 上传。
- [ ] Jakarta Validation、DTO/VO/Entity 边界。
- [ ] `Result<T>` 统一响应与 `WebExceptionAdvice` 全局异常处理。
- [ ] Jackson 日期处理和 Long 转字符串，避免 JavaScript 精度丢失。

入口文件：

- `hmdp-core-service/src/main/java/org/javaup/HmDianPingApplication.java`
- `hmdp-core-service/src/main/resources/application.yml`
- 各框架模块下的 `AutoConfiguration.imports`

### 拦截器与登录上下文

- [ ] `RefreshTokenInterceptor`：取 Token、查 Redis、刷新 TTL、写入上下文。
- [ ] `LoginInterceptor`：登录校验。
- [ ] `MvcConfig`：注册顺序、白名单和拦截范围。
- [ ] `UserHolder`：ThreadLocal 的 set/get/remove。
- [ ] 分布式 Session：多实例服务为什么不能只使用本地 `HttpSession`。

### Spring AOP

- [ ] JDK 动态代理与 CGLIB。
- [ ] 切点、环绕通知和 `ProceedingJoinPoint`。
- [ ] `@ServiceLock`、`@RepeatExecuteLimit` 自定义注解。
- [ ] 方法参数解析、SpEL 锁 Key、多参数组合 Key。
- [ ] AOP 自调用失效与 `AopContext.currentProxy()`。
- [ ] 事务、分布式锁、防重复切面同时存在时的顺序。
- [ ] public 方法限制、代理对象与原对象、异常是否被吞。

重点源码：`ServiceLockAspect`、`RepeatExecuteLimitAspect`、`LockInfoHandleFactory`、`ServiceLockFactory`。

### Spring 事务

- [ ] `@Transactional` 的代理原理、传播行为、隔离级别和回滚规则。
- [ ] `rollbackFor = Exception.class` 与默认回滚规则的区别。
- [ ] 本地事务不能直接保证 MySQL、Redis、Kafka 原子提交。
- [ ] 事务失效：非 public、自调用、异常被吞、对象未被 Spring 管理。
- [ ] 事务内访问外部系统的风险：长事务、网络超时、消息与事务顺序。
- [ ] 用户注册、优惠券创建、订单创建、订单取消、对账状态更新的事务边界。

### 掌握标准

- 能从任意 Controller 追踪到 Service、Mapper、Redis/Kafka。
- 能解释 `@ServiceLock + @Transactional` 的执行顺序。
- 能指出项目中 `AopContext.currentProxy()` 的用途和替代方案。
- 能独立实现一个注解式 AOP 组件并覆盖异常、自调用测试。

---

## MySQL、索引、事务、数据建模

### 数据模型

- [ ] 用户：`tb_user`、`tb_user_info`、`tb_user_phone`。
- [ ] 商户：`tb_shop`、`tb_shop_type`。
- [ ] 社交：`tb_blog`、`tb_blog_comments`、`tb_follow`。
- [ ] 优惠券：`tb_voucher`、`tb_seckill_voucher`。
- [ ] 订单：`tb_voucher_order`、`tb_voucher_order_router`。
- [ ] 一致性：`tb_voucher_reconcile_log`、`tb_rollback_failure_log`。
- [ ] 理解手机号、用户详情、订单路由、对账日志独立建表的原因。

数据库脚本：`sql/hmdp_0.sql`、`sql/hmdp_1.sql`。

### MyBatis-Plus

- [ ] `BaseMapper`、`ServiceImpl`、`IService`。
- [ ] LambdaQuery、LambdaUpdate、条件构造器。
- [ ] 分页查询和 Mapper XML 自定义 SQL。
- [ ] `ORDER BY FIELD` 按 Redis 顺序恢复查询结果。
- [ ] 博客列表逐条查询作者导致的 N+1 问题。
- [ ] SQL 日志与 `local-cache-scope: statement`。

### 索引

- [ ] InnoDB 聚簇索引、二级索引、回表、覆盖索引、索引下推。
- [ ] 联合索引最左前缀原则、范围查询与排序。
- [ ] 唯一索引作为业务幂等的最终防线。
- [ ] `user_id + voucher_id + status` 订单查询索引。
- [ ] `voucher_id + reconciliation_status + create_time` 对账扫描索引。
- [ ] `LIKE`、函数和隐式类型转换造成索引失效。
- [ ] `EXPLAIN`、扫描行数、临时表、文件排序。

### MySQL 事务与并发

- [ ] ACID、MVCC、Undo Log、Redo Log、Binlog。
- [ ] 当前读、快照读；脏读、不可重复读、幻读。
- [ ] 行锁、间隙锁、Next-Key Lock。
- [ ] 乐观锁、悲观锁和库存条件更新。
- [ ] 死锁产生条件、死锁日志、统一加锁顺序。
- [ ] 长事务对版本链、锁和连接池的影响。

### HikariCP

- [ ] 最小空闲连接、最大连接数、连接超时、空闲超时、最大生命周期。
- [ ] 泄漏检测和连接验证。
- [ ] 连接池与实例数、MySQL 最大连接数、SQL 耗时和线程池的关系。

### 实践任务

- 为秒杀订单、路由表、对账日志设计索引并用 `EXPLAIN` 验证。
- 构造两个事务模拟库存竞争，观察锁等待和回滚。
- 核实一人一单是否有数据库唯一约束，并说明只靠应用判断的风险。
- 画出用户注册、异步下单、取消订单的事务边界。

---

## Redis 数据结构、缓存体系和 Lua

### 数据结构与业务映射

| 数据结构 | 项目场景 | 关键操作 |
| --- | --- | --- |
| String | 验证码、库存、空值缓存、限流状态 | GET、SET、INCR、EXPIRE |
| Hash | 登录用户、订阅状态、Redis 对账流水 | HSET、HGET、HGETALL、HDEL |
| Set | 关注关系、一人一单、共同关注 | SADD、SREM、SISMEMBER、SINTER |
| ZSet | 博客点赞、Feed、订阅队列、Top 买家 | ZADD、ZSCORE、ZRANGE、ZRANGEBYSCORE |
| BitMap | 签到和连续签到 | SETBIT、BITFIELD |
| GEO | 附近商户、距离排序 | GEOADD、GEOSEARCH |
| HyperLogLog | UV 基数估算演示 | PFADD、PFCOUNT |

> 实现边界：`ShopServiceImpl` 当前主动将坐标置空，GEO 分支没有真正启用；HyperLogLog 主要出现在测试代码中，不能表述为完整线上业务。

### Redis 工程化封装

- [ ] `RedisCache` 与 `RedisCacheImpl`。
- [ ] `RedisKeyBuild` 与 `RedisKeyManage` 统一 Key 规则。
- [ ] JSON 序列化、泛型对象转换、TTL 设置和查询。
- [ ] String、Hash、Set、ZSet 统一操作和 Scan。
- [ ] Key 命名、业务隔离、版本号和过期策略。
- [ ] 避免线上使用全量 `KEYS`。

### 缓存体系

- [ ] Cache Aside：缓存未命中回源、写回；更新 DB 后删除缓存。
- [ ] 穿透：空值缓存 + 布隆过滤器。
- [ ] 击穿：互斥锁 + 双重检查 + 逻辑过期/异步重建思想。
- [ ] 雪崩：随机 TTL、预热、限流、降级、多级缓存。
- [ ] L1 Caffeine + L2 Redis + DB。
- [ ] Kafka 广播本地缓存失效，DLQ 与 TTL 兜底。
- [ ] 布隆过滤器误判、容量、误判率、增量维护和重建。
- [ ] 热 Key、大 Key、缓存污染、淘汰策略。

源码阅读顺序：

1. `ShopServiceImpl.queryByIdV1`：直接查询 DB。
2. `queryByIdV2`：Redis 与空值缓存。
3. `queryByIdV3`：布隆过滤器。
4. `queryByIdV4`：多级缓存、锁与双重检查。
5. `SeckillVoucherLocalCache`：Caffeine 本地缓存。
6. `SeckillVoucherCacheInvalidationPublisher` 与 `SeckillVoucherInvalidationConsumer`：跨节点失效。

### Lua

- [ ] `KEYS`、`ARGV`、脚本返回码和业务异常映射。
- [ ] Java 通过 `ClassPathResource` 加载脚本。
- [ ] 秒杀脚本：库存、一人一单、扣减、购买记录、流水原子写入。
- [ ] 回滚脚本：恢复库存、释放购买资格、记录恢复流水。
- [ ] Token 脚本：原子比较并删除，保证一次性消费。
- [ ] 滑动窗口和令牌桶 Lua。
- [ ] 长 Lua 阻塞 Redis 的风险。

重点文件：

- `hmdp-core-service/src/main/resources/lua/seckillVoucher.lua`
- `hmdp-core-service/src/main/resources/lua/seckillVoucherRollBack.lua`
- `hmdp-redis-tool-framework/hmdp-redis-rate-limit-framework/src/main/resources/lua/`

### Redis 高可用补充

- [ ] RDB、AOF、混合持久化。
- [ ] 主从复制、复制延迟、Sentinel、Cluster。
- [ ] 脑裂、主从切换数据丢失、跨槽 Lua 限制。
- [ ] 淘汰与过期删除、内存碎片。
- [ ] Redis 宕机时限流失效、缓存回源与数据库保护策略。

### 掌握标准

- 能为每个 Redis Key 说明类型、TTL、写入方和删除方。
- 能手写简化版防穿透、防击穿缓存查询。
- 能逐行解释秒杀/回滚 Lua 和每个返回码。
- 能说明缓存失效消息丢失时如何收敛及遗留风险。

---

## 秒杀限流、扣减、异步下单完整链路

### 完整流程

```text
登录
→ 申请访问令牌
→ IP/用户/活动限流
→ 保存一次性 Token
→ 携带 Token 请求秒杀
→ 再次限流并原子消费 Token
→ 校验活动时间和用户等级
→ 加载 Redis 库存
→ Lua 校验库存和一人一单
→ 原子扣库存、记录用户和扣减流水
→ 生成订单 ID 与 Trace ID
→ Kafka 投递
→ 消费者校验延迟和重复执行
→ MySQL 条件扣库存
→ 创建分片订单、路由、对账日志
→ 手动确认 Offset
→ 异步清理订阅、更新 Top 买家
```

### 四种机制的边界

- [ ] 资格 Token：解决“谁可以进入”。
- [ ] 限流：解决“单位时间允许多少请求进入”。
- [ ] Lua：解决“并发检查和扣减是否原子”。
- [ ] Kafka：解决“请求线程是否同步等待数据库落单”。
- [ ] 幂等：解决“重复请求/消息是否重复生效”。
- [ ] 对账：解决“跨系统局部成功后如何发现和修复”。

### 限流

- [ ] IP、用户、优惠券、接口场景维度。
- [ ] 滑动窗口的精度与 ZSet 成本。
- [ ] 令牌桶的生成速率、桶容量与突发流量。
- [ ] 白名单、临时封禁、违规计数和阈值惩罚。
- [ ] `RateLimitContext`、监听器、惩罚策略和 Micrometer 指标。

### 防超卖和一人一单

- [ ] Redis Set + Lua 原子检查。
- [ ] 数据库条件扣减作为第二道防线。
- [ ] `@RepeatExecuteLimit` 抑制重复执行。
- [ ] 数据库唯一约束作为最终防线。
- [ ] 应用锁不能替代数据库约束和消费幂等。

### 新旧实现辨别

- 普通版 Redis Stream 消费代码仍在 `VoucherOrderServiceImpl`，但启动提交已注释。
- Plus 主链路是 Kafka 异步创建订单。
- 应对比 Redis Stream 与 Kafka 的持久性、确认、扩展性和运维成本。
- 不要把保留的旧版示例方法误认为当前运行路径。

### 故障窗口

| 故障 | 结果 | 处理方式 |
| --- | --- | --- |
| 限流 Redis 不可用 | 限流失败或接口失败 | 明确 fail-open/fail-close，增加熔断保护 |
| Lua 后、Kafka 前宕机 | Redis 少库存、DB 无订单 | 流水扫描、对账、回滚 |
| Kafka 发送结果未知 | 消息可能成功或失败 | 消息幂等、发送确认，可用 Outbox 增强 |
| 消息重复 | 可能重复下单 | 消费幂等、业务唯一约束 |
| DB 成功、ACK 前宕机 | 消息重放 | 消费端幂等 |
| 消息延迟超阈值 | 用户结果超时 | 丢弃、回滚 Redis、写对账日志 |
| DB 扣库存失败 | Redis 已预扣 | 消费失败回调与补偿 |

### 源码顺序

1. `VoucherOrderController`。
2. `SeckillAccessTokenServiceImpl`。
3. `RedisRateLimitHandler`。
4. `VoucherOrderServiceImpl.doSeckillVoucherV2`。
5. `SeckillVoucherOperate` 与秒杀 Lua。
6. `SeckillVoucherProducer`。
7. `SeckillVoucherConsumer`。
8. `VoucherOrderServiceImpl.createVoucherOrderV2`。

### 压测验收

- 库存 10，发送 1000 个多用户并发请求。
- 验证成功订单不超过 10，同一用户最多一单。
- 核对 Redis 库存、DB 库存、订单数和流水。
- 重复投递 Kafka 消息，验证不会重复下单。
- 模拟 Redis、Kafka、MySQL 短暂不可用并记录恢复行为。

---

## Kafka、幂等、重试、补偿和对账

### Kafka 基础与项目配置

- [ ] Broker、Topic、Partition、Replica、Leader、Follower、ISR。
- [ ] Producer、Consumer、Consumer Group、Offset、Rebalance、Consumer Lag。
- [ ] 消息 Key 与分区内顺序。
- [ ] `MessageExtend<T>`：UUID、生产时间、Key、Header、Body。
- [ ] `AbstractProducerHandler` 模板与发送成功/失败钩子。
- [ ] `acks=all`、生产重试、`enable.idempotence=true`。
- [ ] `AbstractConsumerHandler`：反序列化、前置、消费、成功/失败钩子。
- [ ] `enable-auto-commit=false`、`ack-mode=manual_immediate`。
- [ ] 消息延迟检测、失败回滚、缓存失效消费者与 DLQ 消费者。
- [ ] 后置运营任务使用独立线程池，避免阻塞主消费线程。

### 幂等层次

1. 请求：一次性抢购 Token。
2. 接口：`@RepeatExecuteLimit`。
3. Redis：购买 Set + Lua 一人一单。
4. 消息：消息 UUID。
5. 消费：按消息/订单检查重复。
6. 数据库：业务唯一约束和条件写。
7. 补偿：Trace ID 防重复回滚。

> Kafka 幂等生产只处理生产者重试造成的 Kafka 记录重复，不会自动保证 MySQL 业务写入 Exactly Once。

### 重试与死信

- [ ] 生产重试与业务重试的区别。
- [ ] 可重试/不可重试异常、退避策略和重试风暴。
- [ ] DLQ 隔离、告警、人工处置和幂等重放。
- [ ] 缓存失效已有 DLQ 消费入口；秒杀订单的多级重试和 Parking Lot 仍可增强。

### 补偿和对账

- [ ] Redis 扣减/恢复流水与 MySQL 对账日志。
- [ ] Trace ID 关联 Redis、订单和数据库日志。
- [ ] 加载待对账订单，比较 DB 日志和 Redis 流水。
- [ ] DB 有日志而 Redis 无流水：回填 Redis。
- [ ] Redis 有扣减而 DB 无订单：清理流水、释放资格、删除库存触发重建。
- [ ] 对账状态：待处理、一致、异常。
- [ ] 回滚失败日志与告警扩展点。
- [ ] 当前有手动对账接口；生产环境应接入分布式调度、分片执行和告警。

### Outbox 实现边界

- README 描述了 Outbox/本地消息表思想。
- 当前仓库没有完整的 `PENDING → SENT → ACKED/FAILED` 本地消息状态机。
- 面试时应区分“已实现的 Redis 流水 + 对账补偿”和“可演进的完整 Outbox”。

### 必答问题

- `acks=all` 是否意味着消息绝不丢失？
- 消费成功但 ACK 前宕机会怎样？
- 数据库事务提交前 ACK 有何风险？
- 相同 Key 的消息何时有序？
- Kafka Exactly Once 为什么不能直接覆盖 MySQL？
- 对账和补偿为什么也必须幂等？

---

## 分库分表、全局 ID 和路由查询

### ShardingSphere-JDBC

- [ ] ShardingSphere Driver、逻辑表、真实表、实际数据节点。
- [ ] 分库策略、分表策略和分片键。
- [ ] `MOD`、`HASH_MOD`、`INLINE`。
- [ ] 广播表。
- [ ] 非分片键查询的全路由和读扩散。
- [ ] 跨片排序、分页、聚合的归并成本。
- [ ] 跨分片事务限制和数据迁移成本。

配置：`hmdp-core-service/src/main/resources/shardingsphere.yaml`。

### 当前分片设计

| 表 | 分库键 | 分表键/策略 | 关注点 |
| --- | --- | --- | --- |
| `tb_user` | `id` | `id` | 用户 ID 定位 |
| `tb_user_info` | `user_id` | `user_id` | 与用户同路由思路 |
| `tb_user_phone` | `phone` | `phone.hashCode()` | 手机号登录定位 |
| `tb_voucher` | `id` | `id` | 优惠券 ID 路由 |
| `tb_seckill_voucher` | `voucher_id` | `voucher_id` | 秒杀券关联 |
| `tb_voucher_order` | `user_id` | `voucher_id` | 查询维度复杂 |
| `tb_voucher_order_router` | `order_id` | `order_id` | 订单 ID 路由 |
| `tb_voucher_reconcile_log` | `order_id` | `order_id` | 跟随订单定位 |

### 路由表

- [ ] 为什么全局订单 ID 不一定能直接定位订单分片。
- [ ] 创建订单时写入路由记录。
- [ ] 根据 `order_id` 先查询路由再访问订单。
- [ ] 路由记录与订单写入的一致性。
- [ ] 路由表自身的热点和扩展方案。

### 全局 ID

- [ ] 雪花 ID：时间戳、数据中心、机器、序列号。
- [ ] 位移、按位或、同毫秒序列耗尽。
- [ ] 时钟回拨和机器号冲突。
- [ ] 趋势递增对 B+Tree 写入的帮助。
- [ ] 雪花、数据库自增、Redis 自增、号段模式对比。
- [ ] 项目同时保留 `SnowflakeIdGenerator` 和旧 `RedisIdWorker`，需辨别实际路径。

### 实践任务

- 给定用户 ID、手机号、优惠券 ID、订单 ID，手算目标库表。
- 打开 `sql-show`，比较单分片查询和无分片键查询的 SQL 数量。
- 设计“根据订单 ID 查询订单”的路由流程。
- 推演新增节点的数据迁移，解释普通取模为什么不利于扩容。

---

## JVM、线程池、压测、监控与故障演练

### JVM 与线程池

- [ ] JVM 内存区域、对象生命周期、G1、GC 日志、堆/线程转储。
- [ ] ThreadLocal 未清理和队列堆积导致的内存风险。
- [ ] `ThreadPoolExecutor` 参数、核心线程、最大线程、有界队列。
- [ ] `LinkedBlockingQueue` 与 `ArrayBlockingQueue`。
- [ ] `CallerRunsPolicy` 的反压作用和拖慢调用线程风险。
- [ ] 自定义线程工厂、线程命名、未捕获异常处理。
- [ ] CPU/IO 密集型线程数估算。
- [ ] Kafka 消费线程与业务线程池关系。
- [ ] `shutdown`、`awaitTermination`、`shutdownNow`。
- [ ] 常用工具：`jcmd`、`jstack`、`jmap`、JFR。

### 压测

- [ ] 商户冷/热缓存、本地缓存/Redis/DB 三条路径对比。
- [ ] 热点 Key 过期并发，验证只重建一次。
- [ ] 大量不存在 ID，验证布隆和空值缓存。
- [ ] 秒杀多用户、单用户重复、Token 与秒杀接口分开压测。
- [ ] 对比启用/关闭限流的 QPS、P95、P99、错误率。
- [ ] 观察 Kafka Lag、消费者吞吐和 Hikari 使用率。

### 监控

- [ ] Actuator：health、info、prometheus。
- [ ] Micrometer 限流计数。
- [ ] Trace ID、Order ID、Voucher ID、User ID 结构化日志。
- [ ] QPS、错误率、P95/P99。
- [ ] Redis 命中率、重建数、锁等待。
- [ ] Kafka 生产/消费失败率、Lag、DLQ。
- [ ] Hikari 活跃连接、等待和超时。
- [ ] JVM 堆、GC 暂停、线程数。
- [ ] 当前有 Prometheus 基础配置，但没有完整 Grafana Dashboard 和链路追踪。

### 故障演练

| 故障 | 观察 | 验证目标 |
| --- | --- | --- |
| Redis 停止 | 登录、缓存、限流、秒杀 | 是否快速失败，DB 是否被压垮 |
| Kafka 停止 | Redis 已扣减但未投递 | 回调、流水、对账、回滚 |
| MySQL 停止 | 消费端无法落单 | 重试、ACK、Redis 补偿 |
| 消费者暂停 | Kafka Lag | 恢复吞吐、超时消息处理 |
| 重复消息 | 订单数 | 幂等和唯一约束 |
| 删除 Redis 库存 | 懒加载 | DB 库存正确回填 |
| 本地缓存不删 | 多实例读取 | 失效广播与 TTL |
| 慢 SQL | Hikari/接口延迟 | 连接耗尽、超时和限流 |

---

## 安全、测试、部署和工程化

### 安全

- [ ] Token 随机性、TTL、续期和注销。
- [ ] 当前 `logout` 有 TODO，应实现删除 Redis Token。
- [ ] 验证码防刷：手机号/IP 限流、间隔、每日上限、错误次数。
- [ ] 当前验证码会记录日志并直接返回，生产必须移除并接入短信平台。
- [ ] 订单取消、用户信息、订阅操作的资源所有权校验。
- [ ] 上传文件的扩展名、MIME、大小、内容和目录穿越防护。
- [ ] 关注 `last("ORDER BY FIELD...")` 等动态 SQL 拼接。
- [ ] 代理 IP 与 `X-Forwarded-For` 可信链。
- [ ] 数据库密码外置，Token、手机号、验证码日志脱敏。
- [ ] CORS、CSRF、XSS、HTTPS 基础。

### 测试

当前主要测试：`HmDianPingApplicationTests`、`NormalTest`、`RedissonTest`、`RedisRateLimitHandlerTest`。

建议补充：

- [ ] Mockito 单元测试。
- [ ] 登录、拦截器、事务、参数校验集成测试。
- [ ] Testcontainers 管理 MySQL、Redis、Kafka。
- [ ] Lua 返回码、重复请求、库存边界和回滚重入测试。
- [ ] 超卖、一人一单、锁竞争、缓存重建并发测试。
- [ ] Kafka 重复、乱序、延迟、异常和 ACK 窗口测试。
- [ ] 分片路由、广播查询、跨片分页测试。
- [ ] Redis/Kafka/MySQL 故障注入测试。

### 部署与 CI/CD

- [ ] Maven 构建和 Spring Boot 打包。
- [ ] local/test/staging/prod 配置隔离和环境变量外置。
- [ ] MySQL 两库建表和版本迁移。
- [ ] Redis 持久化/高可用，Kafka Topic/分区/副本初始化。
- [ ] 健康检查、优雅停机、多实例部署。
- [ ] 反向代理、HTTPS、静态资源和前端部署。
- [ ] 当前没有完整 Docker Compose、Kubernetes、CI/CD，需要作为补全项。

建议流水线：

```text
提交 → 静态检查 → 单元测试 → 集成测试 → Maven 打包
→ 镜像/依赖扫描 → 测试部署 → 冒烟测试 → 灰度 → 指标观察 → 全量/回滚
```

### 工程质量改进清单

- [ ] 清理或隔离核心类中的旧版 Redis Stream 示例。
- [ ] 拆分体积过大的 `VoucherOrderServiceImpl`、`VoucherServiceImpl`。
- [ ] 消除 `allow-circular-references: true`，梳理循环依赖。
- [ ] 评估 `allow-bean-definition-overriding: true` 是否掩盖 Bean 冲突。
- [ ] 完成登出和 GEO 业务参数。
- [ ] 将 HyperLogLog 扩展为真实 UV 业务，或降低功能声明。
- [ ] 完善 Outbox，或准确描述现有对账补偿边界。
- [ ] 对账接入分布式调度，增加分片、重试、告警和执行记录。
- [ ] 增加数据库业务唯一约束、API 文档、架构决策记录、故障手册。

---

## 综合验收与面试输出

### 必须绘制

- [ ] 模块依赖图。
- [ ] 登录与 Token 刷新时序图。
- [ ] 商户多级缓存流程图。
- [ ] Feed 推送与滚动分页图。
- [ ] 秒杀完整时序图。
- [ ] Kafka 重复消费与 ACK 故障窗口图。
- [ ] Redis/Kafka/MySQL 一致性补偿图。
- [ ] 分库分表路由图。

### 必须沉淀

- [ ] Redis Key 字典：类型、TTL、写入方、读取方、删除时机。
- [ ] 数据库表与索引字典。
- [ ] Kafka Topic 字典：Key、消息体、生产/消费、ACK、失败策略。
- [ ] 订单、优惠券、订阅、对账状态机。
- [ ] 故障矩阵：影响、检测、恢复、遗留风险。
- [ ] 压测报告：环境、数据、并发模型、QPS、延迟、错误率、资源指标。

### 30 秒项目介绍

HMDP-Plus 是本地生活业务项目，核心场景是高并发优惠券秒杀。系统使用 Redis Lua 完成资格判断和库存预扣，通过 Kafka 异步落库，使用消费幂等、Redis 操作流水、数据库对账日志和补偿任务实现最终一致性，同时包含多级缓存、限流、分布式锁、分库分表和延迟通知。

### 深入讲解顺序

1. 流量为什么不能直接进入数据库。
2. Token、限流和 Lua 分别解决什么问题。
3. 为什么使用 Kafka 异步下单。
4. MySQL、Redis、Kafka 为什么不能由本地事务统一提交。
5. 按故障窗口解释幂等、回滚、流水和对账。
6. 说明实现边界及 Outbox、CDC、调度、监控等演进方向。

### 最终掌握标准

- 不看代码能讲清登录、商户查询、Feed 和秒杀下单。
- 能逐行解释关键 Lua。
- 能手算用户、优惠券和订单的分片位置。
- 能证明正常并发下不超卖，并说明极端故障的剩余风险。
- 能修改一个框架模块并补充自动化测试。
- 能区分 README 设计目标、代码已实现能力和待补工程能力。
