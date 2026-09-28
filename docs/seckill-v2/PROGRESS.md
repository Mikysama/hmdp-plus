# Seckill V2 implementation ledger

Approved plan: Redis atomic reservation + database durable acceptance + same-voucher transactions + Outbox + epoch recovery.

- Baseline Maven package succeeded (tests skipped; old integration tests access business data).
- Source snapshot: /tmp/hmdp-seckill-before/source.tar. Repository is unborn; no automatic commit or production migration.
- Redis protocol/tests: delegated, isolated ownership.
- Schema/migration: delegated, isolated ownership.
- Frontend contract/tests: delegated, isolated ownership.
- Root: transactional inventory/request lifecycle, Outbox, recovery, subscriptions, controller/security integration, tests.

Ruling: new implementation is isolated in org.javaup.seckill; old order consumer disabled and controllers switch completely, no dual write.
Ruling: all IDs in public result records are strings; old read-by-voucher endpoint retained only for current owned order.
Ruling: protocol projection sequence is persisted per voucher; rebuild bindings retain original reservation/order and replace epoch.

## Implemented

- Redis remains first for new requests, including separate per-call distributed slots for retries.
- Same-voucher acceptance/create/fail/cancel/adjust state machines and durable Outbox are wired to controllers.
- Monotonic Redis epoch fence, frozen recovery, orphan terminal tombstones, sequence-checked projections and bounded/paged audit are implemented.
- Security/admin defaults, stable HTTP status/error contracts, full catalog validation, owned queries and cancellation, frontend resumable intent are implemented.
- Subscriptions use DB queue/version, Redis+DB acceptance, continuation past ineligible/already-purchased users, and terminal-failure marking.
- Opening reminders are scheduled durably in Outbox and create deduplicated inbox records at delivery time; no reliance on losing Redis delayed messages.
- Kafka CREATE dispatcher is separate from Redis projections; independent lease scheduler and bounded batch/time budgets.
- Config/SQL/migration tools, isolated test profiles, dedicated Kafka/Redis/MySQL tests and limited facade load artifacts are present.

## Evidence and limitations

See README.md, verification.md, monitoring.md and results/*.csv. No production migration, deployment or business-data test writes occurred. Physical MySQL integration exposed and fixed INLINE range and binding-table errors; Spring wiring exposed and fixed legacy KafkaTemplate generic compatibility.

Ruling: activities use explicitly configured Asia/Shanghai DATETIME semantics; tests use explicit UTC. Epoch OPEN is fenced across systems, not an atomic DB/Redis commit.
Ruling: raw performance comparison is facade-level with membership stub and 300s performance window; default60s correctness separately tested. Full HTTP mixed-workload/production capacity acceptance remains a deployment validation requirement, not a claimed throughput guarantee.
Ruling: existing repository is unborn/untracked; preserve source tree, do not auto-commit or clean unrelated files.
