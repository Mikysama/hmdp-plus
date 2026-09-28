# orderId 身份合并验证（2026-09-28）

所有测试使用临时 Redis、KRaft Kafka、H2 或两个临时 MySQL 实例，不连接应用业务数据库。修改保留在工作区，未执行生产升级。

## 验证命令与结果

```bash
python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
bash scripts/run-seckill-v2-mysql-tests.sh -Dmaven.repo.local=.m2/repository \
  '-Dtest=InventoryTest,Seckill*Test,SeckillShardingIT,SeckillEndToEndIT'
```

- Redis 协议：39 个测试通过。
- 离线迁移：12 个测试通过。
- Java：135 个测试，134 通过、1 个显式开关控制的可选压测跳过，0 失败、0 错误。
- 两个临时 MySQL 实例分别验证删列脚本：PROCESSING / 未投递 Outbox 时拒绝；成功后保留成功与取消历史、保留有效购买关系并推进维护代次；重复执行拒绝。
- Redis 并发冒烟：并发 1、8、32、64，每档 1000 次请求、100 份库存，均成功预占 100 次，剩余库存 0，其他 900 次售罄。未修改历史 benchmark-result.json，也不据此宣称端到端吞吐。

## 回归覆盖

请求重试复用 orderId；订单 ID 冲突不覆盖其他预占；旧取消不删新绑定；额外订单预占独立清理；已成交订单不被孤儿核查释放；旧 epoch 不修改新库存；快照中的重复订单被拒绝；大整数 ID 不丢精度；Kafka 只接受新消息版本并基于订单 ID 校验 eventId；数据库提交与消息重投不重复建单。

买家统计使用独立旧前缀，验证准入前缀升级后历史计数、旧订单取消及事件去重仍然有效。代码复核发现的统计前缀兼容问题已修复。

Redis order-only 参数测试先在旧脚本上失败，完成改造后通过；重复订单/快照用例先失败后加入保护；统计前缀回归先观察到新前缀误用导致失败，再拆分配置修复。
