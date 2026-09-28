# 秒杀 V2 数据库与停机迁移

本目录与 `scripts/seckill_v2_migrate.py` 只生成 SQL，**不会自动导入、覆盖或删除现有数据库**。数据库导出命令只执行只读事务。上线必须先停写，再备份、验证、导入新库、切换。不要在旧库原地执行生成的建表语句，不要使用 `mysql --force` 忽略错误。

已有 V2/V3 环境合并预占身份：参见 [orderId 升级说明](../../docs/seckill-v2/order-identity-upgrade.md) 和 `upgrade_order_identity.sql`。全新表结构与旧基础版迁移输出均只保留订单 ID，不保存独立预占编号。

## 路由及表结构约定

同券业务表按 `voucher_id % 2` 分库、`floor(voucher_id / 2) % 2` 分表。`tb_voucher` 按 `id` 使用相同算法。订单和历史对账流水从旧路由迁移至同券路由；订单路由表按旧位置原样保留作历史备份，新链路不用它。

同券业务表与 `tb_voucher` 配置为 bindingTables；异名分片字段通过 `v.id = sv.voucher_id` 等值关联，避免公开券列表和对账 LEFT JOIN 在物理表组合间产生重复或假异常。后台券分页需要 INLINE 算法启用 `allow-range-query-with-inline-sharding`，范围扫描将路由所有相关分片。

`new_tables.sql` 是模板，`__N__` 由脚本替换为 0 和 1，不应直接提交 MySQL。新表为 request、active_purchase、operation、outbox、subscription、recovery、notification。

秒杀券增加 `reserved_stock`、`sold_stock`、`version`、`rule_version`、`admission_epoch`、`admission_state`、`projection_seq`。库存版本与规则版本分别维护，规则判断不能使用每次扣库存都会增长的库存版本。数据库约束要求可用、预占、成交库存非负，且三者之和等于总库存。MySQL 必须支持并执行 CHECK 约束。

**初始准入状态为 `PAUSED`**：空库创建或迁移成功本身不能开放抢购。启动应用后必须显式执行按券受控恢复，完成 Redis 快照、代次与投影水位校验后才进入 `OPEN`。迁移订单生成 `migrated-{orderId}` 请求标识，预占身份直接使用原订单 ID；所有历史订单仍保持原 ID，正常订单建立有效购买关系，取消订单只保留历史请求。

## 新开发环境

从仓库旧 DDL 提取所有表结构（不执行旧文件中的 DROP 或种子数据），加上 V2 字段及新表：

```bash
python3 scripts/seckill_v2_migrate.py init \
  --target-schemas hmdp_v2_0 hmdp_v2_1 \
  --output-dir /tmp/hmdp-v2-empty
```

输出 `init_0.sql`、`init_1.sql` 和 SHA-256 清单。选择各自的目标 MySQL 实例导入，两个库可以位于独立实例。目标库名必须尚不存在，生成 SQL 使用 `CREATE DATABASE` 和 `CREATE TABLE`，存在时立即报错。空环境用户、店铺等基础数据需使用独立测试种子，不能把不经验证的旧订单数据灌入 V2 表。

## 旧环境迁移

1. 关闭秒杀、取消、调库存、订阅写入，停止旧 Kafka 消费者、自动补发、初始化及对账任务。确认没有仍在提交的旧事务。两个源库无法建立跨实例统一快照，因此停写是导出一致性的前提。
2. 使用运维备份工具保存两个源库、Redis 状态、Kafka offset；保留原库，不删除旧 Topic。迁移脚本导出是补充审计，不替代备份。
3. 在独立工具虚拟环境安装 PyMySQL。以仅有 SELECT 权限的账号提供以下环境变量，避免在命令行暴露密码：`SECKILL_SOURCE_0_HOST/PORT/USER/PASSWORD` 和 `SECKILL_SOURCE_1_HOST/PORT/USER/PASSWORD`。PORT 默认 3306；其余没有默认值。

```bash
python3 scripts/seckill_v2_migrate.py export \
  --source-schemas hmdp_0 hmdp_1 \
  --output-dir /secure-backups/seckill-v2-source
```

导出独立只读一致快照，每个源库的所有基础表结构及行数据保存在 `shard_0.json`、`shard_1.json`；导出 session 固定 UTC，保留 timestamp 精度。目录已存在时拒绝覆盖。导出中断时没有完整 manifest，不能进入 prepare；使用新的空目录重新导出。源数据包含用户资料，应按数据库备份权限保护。

时间语义必须提前核对：MySQL `TIMESTAMP` 在 UTC 导出、UTC 导入后保持同一时刻；`DATETIME` 不带时区，脚本原样保留其墙上时间，不自动假设为 UTC。如果部署中已把活动 begin/end 改成 DATETIME，必须保持源业务时区语义，并令 `seckill.v2.activity-zone`、JDBC connectionTimeZone、MySQL session time_zone 三者一致（默认 Asia/Shanghai）。生产配置通过 `forceConnectionTimeZoneToSession=true` 固定数据库 session 时区；隔离测试显式使用 UTC。不能把 DATETIME 时区语义与 Redis 订阅时间戳的 UTC 导出混为一谈。历史订单迁移请求时间用于历史展示而非重新排队；运行中新的请求 expires_at 一律按活动时区的数据库当前时间写入。

4. 同时导出旧 Redis 订阅以及 Redis/MQ 待处理申请为 JSON。订阅导出为数组，每条至少 `{ "voucher_id": 5, "user_id": 9, "subscribed_at": "2026-09-24 03:00:00.123" }`，时间统一 UTC，保留原 ZSet 时间。重复记录或缺失原排队时间会被拒绝，不能自行填写当前时间。待处理导出为数组，保存来源及原始定位字段，有订单号时使用 `order_id`；脚本将未落库项标记 `TERMINATED_AT_MIGRATION`，不会创建订单或加回数据库库存。旧消息已落库则审计为 `ALREADY_PERSISTED`。
5. 离线验证并生成新库导入包：

```bash
python3 scripts/seckill_v2_migrate.py prepare \
  --source-dir /secure-backups/seckill-v2-source \
  --target-schemas hmdp_v2_0 hmdp_v2_1 \
  --subscriptions /secure-backups/subscriptions.json \
  --pending /secure-backups/pending.json \
  --output-dir /secure-backups/seckill-v2-stage
```

没有订阅或待处理数据时，可以省略相应参数；manifest 会记录未提供。实际有旧状态时必须提供，省略并不代表已审计完毕。

验证包括：源文件校验和、目标与源库不同、所有表中券关联存在、订单 ID 唯一、正常订单购买关系唯一、库存守恒、订阅唯一及原时间存在。任一错误都在生成 SQL 前停止。源表非正常/取消状态订单也拒绝迁移，避免默默改变未实现的支付或核销语义。旧订单与对账流水完整保留；旧路由表及所有非迁移业务表在原库编号内原样复制。

6. 查看 `counts.json` 和 `terminated_pending.json`，核对迁移清单。备份输出目录后，将 `stage_0.sql`、`stage_1.sql` 分别导入**新的**物理数据库。SQL 用 UTF-8 hex 文本字面量避免单双引号、反斜杠或 SQL mode 对数据造成改变。源库不被修改；导入失败保留目标库作诊断，不自动 DROP 或覆盖，修复后选择全新目标库名生成新包。
7. 使用只读查询核对两库每张券库存守恒、订单总数、正常订单与 active_purchase 数量、旧对账流水数量，以及新路由下每笔订单位置。不要因 CHECK 约束通过就跳过全量记录数量核对。
8. 将 ShardingSphere 的两个数据源 URL 指向新库（端口可以不同），同时部署 V2 前后端，保持旧消费者停用。V2 消费组、Topic 和 Redis 命名空间必须隔离。仅 `PAUSED` 的合法迁移状态可由受控恢复开放；异常库存状态由人工处理，禁止自动把数据库数覆盖为猜测值。
9. 执行按券重建及冒烟：抢购、重复请求、取消重试、取消后重购、旧消息重放不影响新订单；通过后开放写入口并监控。

开放写入前可切回旧配置并恢复备份。开放写入后必须停写、保留 V2 新订单和流水，再修复或专门反向迁移；禁止直接用旧快照覆盖。

## 本地隔离验证

纯 Python 测试无外部依赖：

```bash
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
```

MySQL 集成工具需要本机 `mysqld/mysql/mysqladmin`，不访问默认 3306，不需要 Docker：

```bash
# 仅在两个全新独立 MySQL 实例验证全部 DDL
SECKILL_SCHEMA_ONLY=1 bash scripts/run-seckill-v2-mysql-tests.sh

# 加载同样的 DDL，然后执行 Java Sharding 集成测试
bash scripts/run-seckill-v2-mysql-tests.sh
```

脚本分配临时端口和临时 datadir；默认成功后清理，失败保留日志，`SECKILL_KEEP_TEST_DATA=1` 可保留成功运行的数据。默认无论成功失败都关闭本次启动的 mysqld。连续故障验证需要复用实例时，显式使用 `SECKILL_KEEP_TEST_SERVERS=1` 保留进程，并读取输出目录的 `connections.txt`；验证结束后必须终止其中记录的 PID。`SECKILL_TEST_TMPDIR` 可选择已被本机 MySQL 安全配置允许的临时目录，默认 `/tmp`，不要为了提速修改宿主安全策略。Java 测试使用 system properties `seckill.test.mysql0`、`seckill.test.mysql1`、`seckill.test.mysqlUser`、`seckill.test.mysqlPassword`；不得无配置时回退到应用开发库。脚本的临时 root 空密码只用于本机隔离测试，不能用于业务部署。
