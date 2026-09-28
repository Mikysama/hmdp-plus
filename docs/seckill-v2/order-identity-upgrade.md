# 预占身份统一为 orderId（2026-09-28）

Redis 预占现在仅使用字符串 `orderId` 标识：users 的值、requests 的值、reservations 的字段、pending/inflight 的成员均为订单 ID。`Reservation` 仍表示预占记录，但不再包含独立的 `reservationId`。requestId 的幂等语义、epoch 防旧代次和 projection_seq 顺序保持不变。数据库请求表的 id 就是候选订单 ID；有效购买表继续通过 order_id 关联请求。

Kafka 新 Topic/消费组为 `seckill-admission-v4`，消息 schemaVersion=4、eventId=`admit:{orderId}`；DLT 为 `seckill-admission-v4.DLT`。生产者和消费者一起升级。旧 V3 消息不能转投 V4；本版拒绝旧 schema，不提供在线混用模式。旧 V2 建单消费者仅保留已有排空入口，不承担 V3 消息兼容。

默认 Redis 前缀由 `hmdp:v2` 改为 `hmdp:v3`（配置项仍叫 seckill.v2.prefix）。Lua 资源继续放在 lua/v2 目录，部署时按应用版本整体替换。若环境覆盖 prefix，必须使用一个全新、没有旧 UUID 绑定的前缀。保留旧准入命名空间作审计，不把旧准入数据直接复制到新前缀。买家统计协议没有改变，单独通过 `seckill.v2.buyer-stats-prefix` 配置，默认仍为 `hmdp:v2`；如果原环境自定义过前缀，此配置必须设置为原值，以保留买家计数、订单终态和事件去重历史。

## 已有 V2/V3 数据库升级

1. 关闭提交、取消、订阅、调库存等外部写入，停止自动补发产生新请求（有 WAITING 订阅时先按业务规则取消/冻结），保留旧消费者和后台投影/超时核查运行以完成排空。记录并核查 V2/V3 Kafka lag 和 DLT；死信不能直接丢弃。
2. 确认旧 Redis 没有未核查 HELD，数据库没有 PROCESSING 或 reserved_stock，非 REMINDER Outbox 全部 SENT；再停止所有旧应用与后台任务。排队失败及取消请求的数据库历史必须保留，防止迟到请求复活。
3. 备份两个物理库、旧 Redis 与 Kafka offset。审核 `sql/v2/upgrade_order_identity.sql`，在每个物理库各执行一次；不通过 ShardingSphere 执行。脚本检查旧字段、待处理请求和 Outbox，删除请求及有效购买表的冗余列，并令 OPEN 活动进入 REBUILDING。它不连接或操作 Redis/Kafka，也不能自动证明它们已排空。
4. MySQL DDL 非事务性；脚本不能使用 --force。若任何一库部分失败，保持停写，核对备份与实际表结构后修复，不能直接重复或开放流量。脚本拒绝已升级/部分升级的列状态。两库必须全部成功后再启动新版。
5. 创建 V4 Topic/DLT、配置 ACL，使用新 Redis 前缀启动新版。后台以数据库当前可用库存、有效订单、请求历史和投影水位重建。已有订单和请求 ID 保留；不直接按初始库存回填。
6. 检查每券数据库 OPEN、Redis epoch/sequence/stock 和用户绑定一致。验证重复请求、取消重试、取消后重购、旧代次事件拒绝及通知统计。完成检查后开放入口。PAUSED/INVARIANT 活动需要先人工核查。
7. 新版开放写入后，不能直接用旧快照覆盖新订单。旧版本无法使用删除字段后的表结构；回退需停写并制定包含新增数据的恢复方案。

全新环境继续使用 `scripts/seckill_v2_migrate.py init` 创建无冗余列的表。该工具的 `prepare` 是旧基础版订单迁移工具，不是现有 V2/V3 请求历史升级工具；已有 V2/V3 数据应走上述 DDL 流程，避免重造历史请求。

## 校验

- Redis 协议测试覆盖重试保留订单 ID、旧释放不清理新购买、重复订单 ID 拒绝、快照重建、超大 ID 精度和 epoch 隔离。
- Java 测试覆盖额外订单预占清理、提交后孤儿核查不释放成功订单、Kafka 新协议/重投、取消幂等及同券事务。
- 两个临时 MySQL 实例验证分片及端到端流程；不使用应用开发库。
