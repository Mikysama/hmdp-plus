# Order identity consolidation implementation plan

**Goal:** Remove the separate reservation identifier; orderId identifies each Redis hold and intended database order. User approved this design in the conversation.

**Architecture:** Preserve requestId retry semantics, epoch fencing and projection sequence. Key Redis user/request/hold/pending/inflight data by string orderId. Compare incoming orderId with database request.id when releasing extra holds. Remove duplicate database columns and wire fields. Isolate incompatible Redis state with hmdp:v3 and publish schemaVersion=4 on seckill-admission-v4. Upgrade requires stopping new submissions, draining old messages and Outbox, stopping old workers, applying reviewed DDL and rebuilding from the database; no live service or database mutations during this task.

**Workspace:** Existing repository has an unborn history and all project files are untracked. Edit in place; preserve original files in /tmp/hmdp-order-identity-before.tar.gz, do not create a baseline commit or move user files.

## Tasks
- [x] Convert Redis protocol tests to the order-only API; observe failure with old Lua.
- [x] Remove duplicate identity from Java model, Kafka, transactions, Outbox, recovery, Lua, DDL and offline migration output.
- [x] Update existing tests and add extra-order cleanup / old-cancel / protocol validation regressions.
- [x] Add stopped-service database upgrade DDL and document new namespace/topic and drain/rebuild procedure.
- [x] Run isolated Redis and migration tests, Maven InventoryTest/Seckill*Test, and isolated two-MySQL integration tests when available. Review source diff against snapshot and search remaining old identity references.

Review correction: admission namespace upgrades must not reset buyer statistics. Added separate buyer-stats-prefix default hmdp:v2, with wiring and real Redis history/cancellation regressions.
