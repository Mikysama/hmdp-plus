# Redis Outbox Stream verification — 2026-09-28

Only disposable Redis, Kafka KRaft, H2 and two temporary MySQL servers were used; no business database migration or deployment was performed.

## Commands and results

- `python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'`: final 44 tests pass.
- `python3 -m unittest discover -s sql/v2 -p 'test_*.py'`: 12 tests pass.
- `bash scripts/run-seckill-v2-mysql-tests.sh -Dmaven.repo.local=.m2/repository '-Dtest=InventoryTest,Seckill*Test,SeckillShardingIT,SeckillEndToEndIT'`: 142 Java tests, zero failures/errors, one opt-in load test skipped. Includes actual Kafka delivery from Stream after failed HTTP publication, duplicate message identity, and terminal orphan replay against two physical MySQL instances. Both temporary databases also passed the existing schema-upgrade fixture checks.
- Following review, added immutable autoIssue retry validation and its regression. Final `mvn -q -pl hmdp-core-service -am test -Dmaven.repo.local=.m2/repository '-Dtest=InventoryTest,Seckill*Test' -Dsurefire.failIfNoSpecifiedTests=false`: 130 tests pass, zero failures/errors/skips. This includes 6 real-Redis relay tests plus Kafka transport, transaction, Outbox and recovery regressions. The final mode-only follow-up did not rerun the two-MySQL suite.
- Redis concurrent smoke at concurrency 1/8/32/64, each 1000 requests and stock 100: exactly 100 holds, 100 Stream entries, 900 SOLD_OUT, remaining stock 0. Historical benchmark result file was not overwritten; this is no end-to-end throughput claim.

## Failure coverage

- Lua admission persists its original delivery intent; request retry never appends another entry.
- Wrong Stream key type rejects before stock or user-binding mutation.
- autoIssue survives relay and cannot change on retry.
- Failed Kafka ACK retains the entry; a new relay instance retries without HTTP resubmission.
- ACK followed by Redis deletion failure causes safe same-identity retransmission.
- Malformed payload is retained, while later valid entries are processed.
- A high-water boundary guarantees a scan pass wraps to earlier failures despite new arrivals.
- Redis epoch rebuild preserves pending Stream records; database epoch checks handle late delivery.
- Existing failed request cannot be resurrected; duplicate delivery creates exactly one order.

Tests exposed continuous-arrival retry starvation and conflicting retry modes before fixes. Both regressions now pass. Independent read-only code review found no remaining correctness issue after the mode fix.

Logs: `/tmp/outboxstream-full.log`, `/tmp/outboxstream-final-java.log`, `/tmp/outboxstream-fairness-red.log`, `/tmp/outboxstream-mode-red.log`. Pre-change workspace snapshot: `/tmp/hmdp-before-outboxstream.tar.gz`.
