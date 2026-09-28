# Redis V2 admission protocol verification

Run from repository root:

```sh
python3 tests/redis_v2/test_protocol.py
python3 tests/redis_v2/benchmark.py
```

Both commands start their own temporary Redis server with TCP disabled, persistence
disabled and a private UNIX socket. They do not connect to application Redis.
`redis-server` and `redis-cli` must be installed; Python uses a small RESP client
from the test fixture and has no third-party dependencies.

The benchmark runs 100,000 distinct users against stock 100 separately at
concurrency 1, 8, 32 and 64. It asserts exactly 100 successful holds per run and
99,900 SOLD_OUT responses, and exactly 100 persisted Outbox Stream entries. Admission rate/concurrency limits are intentionally
raised for this stock-screening test. Latencies include Python scheduling and
local UNIX-socket transport. `benchmark-result.json` records measured output;
these are **not HTTP, database or Kafka throughput claims**. Whole-chain,
multi-instance database routing, failure injection and direct-database baseline
benchmarks must be run separately in an isolated integration environment.

## Gateway contracts

`RedisAdmissionGateway.Reservation` is the immutable transport record. Snowflake
IDs are stored as strings in Lua JSON to avoid IEEE-754 truncation. Jackson maps
voucher/user/epoch/ruleVersion strings into the Java long record fields.

* `reserve` never calls the database. Unknown/missing keys, failed lease, invalid
  token, exhausted rate/inflight capacity and insufficient stock fail closed.
* `project` returns APPLIED, DUPLICATE or STALE; sequence gaps and invalid
  reservation ownership throw. Database projection sequence must be dedicated
  to Redis events, not shared with unrelated Kafka/notification events.
* `rebuild` requires a database-frozen voucher and a newer epoch. Batches of at
  most 100 bindings stage a namespace protected by a worker token. Only ACCEPTED
  and COMMITTED database bindings belong in the snapshot. Activation is separate.
* `activate` and `renew` verify all mandatory structures; they cannot silently
  repair deleted reservation maps. Renewal should follow authoritative database
  OPEN/epoch verification, not only Redis inspection.
* `releaseSlot` clears the admission concurrency lease only. It must never be
  confused with releasing inventory. Use the Reservation overload so the slot
  belongs to the exact epoch.
* `overdue` returns HELD records older than max(orphan-seconds, queue-seconds),
  currently 60 seconds. Expiry is a signal for
  database arbitration, never permission to return inventory directly.
* `inspect` is a diagnostic read, not a transactional consistency snapshot.

The projection state machine prevents late ACCEPT/COMMIT from resurrecting
RELEASED reservations. COMMITTED can only be released by CANCEL. User binding
removal is compare-by-orderId, so an old release never clears a new purchase.

## Recovery fence and bounded reconciliation

A voucher-wide `:fence` stores the highest staged epoch. BEGIN raises it atomically;
stale BEGIN/APPEND/FINISH, ACTIVATE, RENEW and admission cannot cross that fence.
This prevents an old recovery worker from reopening its READY epoch after a newer
worker has started staging but has not yet activated. The database recovery owner
and epoch must still be checked by the coordinator before activation.

`available()` uses PING with no business keys. Call it before freezing a voucher
solely to rebuild unavailable Redis; an outage by itself must not block existing
database-order progress.

`auditProjection(voucher, epoch, sequence, total)` atomically verifies the exact
matching database epoch/sequence, stock conservation with live reservations, and
both directions of user/request binding. A concurrent version or more than 2,000
stored records returns BUSY (incomplete audit), never MATCH. CORRUPT failures should
trigger controlled recovery after rechecking database state. The bounded check
avoids an unbounded HGETALL script blocking the Redis event loop. Large histories
need a separate paged reconciliation/frozen snapshot before claiming full audit.

`projectBuyerStats(shop, originalPurchaseDate, user, order, eventId, kind)` supports
SUCCESS/CANCEL. It stores an event fingerprint and an order's terminal state;
CANCEL dominates late SUCCESS and an old order cannot decrement a later order.
Scores remain nonnegative. Data and event dedup records have no automatic TTL.
All three shop/day keys share a Redis Cluster hash tag. This projection is separate
from inventory sequencing; callers must use the original purchase day on cancel.

## Full audit for large vouchers and corrupted authority keys

`auditProjectionPaged(voucher, epoch, sequence, total)` scans reservation, user and
request hashes in pages of approximately 100. It counts unique live order
IDs, validates both mapping directions and checks final stock conservation.
A metadata mutation counter is incremented by every successful admission and
projection change; every audit page and final check verify the same counter.
Concurrent changes return BUSY for retry. This provides a complete path beyond
the 2,000-row fast-audit cap without one unbounded Lua invocation. Callers should
schedule retries or a controlled admission pause if a very hot voucher prevents
an optimistic full scan from completing.

Only trusted `rebuild` BEGIN may repair a malformed active pointer/fence. It first
rejects any valid higher fence or active epoch, then clears malformed authority
state and stages the new epoch. Admission and projection scripts do not repair
these keys. Wrong-type stock/user keys in an inactive epoch are replaced only as
part of the already-authorized database-frozen rebuild.

## Per-HTTP-attempt concurrency bound

`tryEnter(voucherId, attemptId)` and `leave(voucherId, attemptId)` use an independent
same-voucher ZSet lease. Generate a fresh server UUID for **every HTTP call**,
including repeated calls with the same business requestId. Acquire before reserve;
release in finally. Capacity uses `seckill.v2.admission-concurrency` (default 32),
lease is 30 seconds, full capacity returns false without a waiting queue. Redis
unavailability or wrong types fail closed. Re-entering the same attempt ID is
idempotent and does not extend its lease. This controls concurrent attempts even
when all of them replay a single already-held reservation. The original
reservation inflight lease remains as an additional bound.

The 30-second lease is an operational bound, not a distributed database lock:
callers must bound request and transaction execution below that duration. A
stalled process can outlive its lease; business correctness still relies on the
database voucher lock and persistent idempotency.

The order-only protocol keys holds and user/request bindings by string orderId. Lua reserve takes epoch,voucher,user,request,token,order,rate,capacity,slotTtl; project takes epoch,sequence,event,kind,user,order,delta. The reservations hash stores hold records, without a separate reservation identifier. Use a fresh Redis prefix when upgrading.

Admission now writes an Outbox Stream in the same Lua execution. The 12th key is the stable per-voucher `:admission-outbox`; optional argument 10 is `autoIssue` (default false). Tests cover retry deduplication, wrong key type before stock mutation, auto-issue identity and rebuild survival.
