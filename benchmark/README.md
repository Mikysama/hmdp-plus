# HMDP-Plus benchmark

This directory contains repeatable local performance tests. Keep the database,
Redis, Kafka, application and k6 on the same machine when comparing runs, and
record the hardware/JVM settings with every result.

The cached shop test uses a constant arrival rate so that the requested load is
separate from the latency of the system under test:

```bash
BASE_URL=http://127.0.0.1:8085 SHOP_ID=1 RATE=1000 DURATION=60s \
  k6 run --summary-export benchmark/results/shop-query.json benchmark/shop-query.js
```

Run a warm-up before recording results. Increase `RATE` in steps, and stop using
a result for a resume when `dropped_iterations` is non-zero or the business
success check starts failing. Report QPS, P95, P99 and error rate together.

The end-to-end seckill test requires a dedicated voucher and deterministic Redis
login sessions. For example, for a 100 flows/s, 30-second run:

```bash
bash benchmark/prepare-e2e-sessions.sh e2e100 4000
RUN_ID=e2e100 VOUCHER_ID=2 RATE=100 DURATION=30s \
  k6 run --summary-trend-stats 'avg,min,med,max,p(90),p(95),p(99)' \
  --summary-export benchmark/results/seckill-e2e-100.json \
  benchmark/seckill-e2e.js
```

`RATE` is business flows per second. Each flow issues a one-time access token and
then submits one seckill request, so the HTTP request rate is approximately twice
the configured flow rate. After the run, wait for Kafka consumption and compare
accepted requests with database orders, reconciliation logs and remaining stock.
