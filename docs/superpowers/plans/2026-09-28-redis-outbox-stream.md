# Redis admission Outbox Stream implementation plan

Goal: persist Kafka delivery intent atomically with Redis admission so browser retries are no longer required for eventual delivery attempts.

Design: each voucher has a stable `:{voucher}:admission-outbox` Stream, independent of epoch. Reserve Lua validates types before mutation, appends the original reservation and autoIssue flag, and does not append on request retries. HTTP retains synchronous Kafka ACK semantics. A dedicated bounded scheduler scans streams with XRANGE and deletes entries only after ACK. Multiple relays may duplicate delivery; existing database identity/epoch/terminal checks are authoritative. Scan cursors wrap so failed or malformed records cannot starve later records. No MAXLEN or expiry may discard undelivered records. Existing queue expiry and orphan failure remain business deadlines, not unlimited completion promises.

- [x] Add failing Redis protocol tests: atomic intent, retry deduplication, wrong Stream type, epoch rebuild survival, autoIssue persistence.
- [x] Add reserve Stream support and gateway bounded read/ack methods; preserve 5-argument reserve compatibility for internal tests.
- [x] Add dedicated relay with bounded scan, retry by retention, validation, ACK-only delete and metrics.
- [x] Test relay with real Redis and Kafka publication failures, restart, lost delete and scan fairness; verify end-to-end without HTTP publication.
- [x] Run Redis, Java, two-MySQL/Kafka end-to-end regression tests and document deployment/persistence requirements.

No production database migration or commit is part of this change. Existing namespace can be retained, but stop/drain old writers before deployment: old Lua cannot write Stream records for old pending holds.

Review follow-up: persist the original autoIssue in the raw hold and reject conflicting retry modes; preserve eight-field Java/Kafka identity. Added regression first (failed), then fixed and reran Redis + Java tests. Fixed cursor starvation with per-pass high-water bounds after a failing continuous-arrival test.
