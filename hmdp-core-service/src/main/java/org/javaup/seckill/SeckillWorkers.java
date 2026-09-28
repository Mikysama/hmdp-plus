package org.javaup.seckill;

import static org.javaup.seckill.SeckillStore.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import lombok.extern.slf4j.Slf4j;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
    name = "seckill.v2.workers-enabled",
    havingValue = "true",
    matchIfMissing = true)
public class SeckillWorkers {
  private final SeckillStore s;
  private final SeckillTransactions tx;
  private final RedisAdmissionGateway redis;
  private final SeckillRecovery recovery;
  private final SeckillFacade facade;
  private final KafkaTemplate<String, String> kafka;
  private final MeterRegistry metrics;
  private final SeckillCatalogService catalog;
  private long dispatchCursor;
  private long kafkaCursor;
  private final Set<Long> repairNeeded = java.util.concurrent.ConcurrentHashMap.newKeySet();

  public SeckillWorkers(
      SeckillStore s,
      SeckillTransactions tx,
      RedisAdmissionGateway redis,
      SeckillRecovery recovery,
      SeckillFacade facade,
      @org.springframework.beans.factory.annotation.Qualifier("seckillV2Template")
          KafkaTemplate<String, String> kafka,
      MeterRegistry metrics,
      SeckillCatalogService catalog) {
    this.s = s;
    this.tx = tx;
    this.redis = redis;
    this.recovery = recovery;
    this.facade = facade;
    this.kafka = kafka;
    this.metrics = metrics;
    this.catalog = catalog;
  }

  void each(LongConsumer fn) {
    long after = 0;
    while (true) {
      var page = s.vouchers(after, 100);
      if (page.isEmpty()) return;
      for (long v : page) {
        try {
          fn.accept(v);
        } catch (Exception e) {
          metrics
              .counter("seckill_v2_worker_failure", "type", e.getClass().getSimpleName())
              .increment();
          log.warn("Seckill background task failed voucher={}", v, e);
        }
      }
      after = page.get(page.size() - 1);
    }
  }

  @Scheduled(
      fixedDelayString = "${seckill.v2.lease-scan-ms:1000}",
      scheduler = "seckillLeaseScheduler")
  public void maintain() {
    each(
        v -> {
          var row = s.voucher(v, false);
          if ("PAUSED".equals(str(row, "admission_state"))) {
            var task = s.one("SELECT phase FROM tb_seckill_recovery WHERE voucher_id=?", v);
            if (task != null && "INVARIANT".equals(str(task, "phase"))) return;
          }
          if (!"OPEN".equals(str(row, "admission_state"))) {
            repairNeeded.add(v);
            return;
          }
          try {
            if (!redis.renew(v, number(row, "admission_epoch"))) repairNeeded.add(v);
          } catch (RuntimeException e) {
            repairNeeded.add(v);
          }
        });
  }

  @Scheduled(fixedDelayString = "${seckill.v2.recovery-scan-ms:5000}")
  public void repair() {
    each(
        v -> {
          if (repairNeeded.contains(v)) {
            recovery.recover(v);
            repairNeeded.remove(v);
          }
        });
  }

  @Scheduled(fixedDelayString = "${seckill.v2.expiry-scan-ms:5000}")
  public void expire() {
    each(
        v -> {
          if (!"OPEN".equals(str(s.voucher(v, false), "admission_state"))) return;
          var rows =
              s.jdbc.queryForList(
                  "SELECT id FROM tb_seckill_request WHERE voucher_id=? AND status='PROCESSING' AND"
                      + " expires_at<=CURRENT_TIMESTAMP ORDER BY expires_at LIMIT 100",
                  v);
          for (var r : rows) tx.expire(v, number(r, "id"));
          try {
            for (var r : redis.overdue(v, System.currentTimeMillis(), 100)) tx.resolveOrphan(r);
          } catch (RuntimeException e) {
            log.warn("Redis orphan scan unavailable voucher={}", v, e);
          }
        });
  }

  @Scheduled(fixedDelayString = "${seckill.v2.audit-scan-ms:60000}")
  public void audit() {
    each(
        v -> {
          try {
            recovery.audit(v);
          } catch (SeckillFailure failure) {
            metrics.counter("seckill_v2_audit_failure", "code", failure.getCode()).increment();
            log.error("Seckill audit voucher={} code={}", v, failure.getCode());
          }
          var now = s.now(v);
          var old =
              s.one(
                  "SELECT create_time,next_attempt_at,event_type FROM tb_seckill_outbox WHERE"
                      + " voucher_id=? AND status<>'SENT' AND next_attempt_at<=CURRENT_TIMESTAMP"
                      + " ORDER BY create_time LIMIT 1",
                  v);
          if (old != null
              && ("REMINDER".equals(str(old, "event_type"))
                      ? time(old, "next_attempt_at")
                      : time(old, "create_time"))
                  .isBefore(now.minusSeconds(30))) {
            metrics.counter("seckill_v2_outbox_overdue").increment();
            log.error("Outbox delay exceeds 30s voucher={}", v);
          }
          var expired =
              s.one(
                  "SELECT expires_at FROM tb_seckill_request WHERE voucher_id=? AND"
                      + " status='PROCESSING' ORDER BY expires_at LIMIT 1",
                  v);
          if (expired != null && time(expired, "expires_at").isBefore(now.minusSeconds(10))) {
            metrics.counter("seckill_v2_request_overdue").increment();
            log.error("Request exceeds expiry by 10s voucher={}", v);
          }
          try {
            var held = redis.overdue(v, System.currentTimeMillis(), 1);
            if (!held.isEmpty()) {
              metrics.counter("seckill_v2_orphan_overdue").increment();
              log.warn(
                  "Unpersisted reservation age={}ms voucher={}",
                  System.currentTimeMillis() - held.get(0).createdAt(),
                  v);
            }
          } catch (RuntimeException unavailable) {
            metrics.counter("seckill_v2_projection_unavailable").increment();
          }
        });
  }

  @Scheduled(fixedDelayString = "${seckill.v2.outbox-scan-ms:500}")
  public void dispatch() {
    dispatchBatch(false);
  }

  @Scheduled(fixedDelayString = "${seckill.v2.outbox-scan-ms:500}")
  public void dispatchKafka() {
    dispatchBatch(true);
  }

  void dispatchBatch(boolean kafkaOnly) {
    long cursor = kafkaOnly ? kafkaCursor : dispatchCursor;
    var page = s.vouchers(cursor, 100);
    if (page.isEmpty()) {
      if (kafkaOnly) kafkaCursor = 0;
      else dispatchCursor = 0;
      return;
    }
    int remaining = 100;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    for (long v : page) {
      if (kafkaOnly) kafkaCursor = v;
      else dispatchCursor = v;
      var rows =
          s.jdbc.queryForList(
              "SELECT * FROM tb_seckill_outbox WHERE voucher_id=? AND event_type "
                  + (kafkaOnly ? "=" : "<>")
                  + " 'CREATE' AND status<>'SENT' AND next_attempt_at<=CURRENT_TIMESTAMP AND"
                  + " (lease_until IS NULL OR lease_until<CURRENT_TIMESTAMP) ORDER BY"
                  + " create_time,event_id LIMIT ?",
              v,
              remaining);
      for (var row : rows) {
        String owner = UUID.randomUUID().toString(), id = str(row, "event_id");
        int claimed =
            s.jdbc.update(
                "UPDATE tb_seckill_outbox SET"
                    + " status='SENDING',lease_owner=?,lease_until=?,attempts=attempts+1 WHERE"
                    + " voucher_id=? AND event_id=? AND status<>'SENT' AND (lease_until IS NULL OR"
                    + " lease_until<CURRENT_TIMESTAMP)",
                owner,
                s.now(v).plusSeconds(30),
                v,
                id);
        if (claimed != 1) continue;
        remaining--;
        try {
          deliver(row);
          s.jdbc.update(
              "UPDATE tb_seckill_outbox SET"
                  + " status='SENT',lease_until=NULL,update_time=CURRENT_TIMESTAMP WHERE"
                  + " voucher_id=? AND event_id=? AND lease_owner=?",
              v,
              id,
              owner);
        } catch (Exception e) {
          long backoff = Math.min(30, 1L << Math.min(5, number(row, "attempts")));
          s.jdbc.update(
              "UPDATE tb_seckill_outbox SET"
                  + " status='PENDING',lease_until=NULL,next_attempt_at=?,update_time=CURRENT_TIMESTAMP"
                  + " WHERE voucher_id=? AND event_id=? AND lease_owner=?",
              s.now(v).plusSeconds(backoff),
              v,
              id,
              owner);
          metrics.counter("seckill_v2_outbox_retry", "type", str(row, "event_type")).increment();
          log.warn("Outbox retry event={} reason={}", id, e.toString());
        }
        if (remaining <= 0 || System.nanoTime() > deadline) return;
      }
      if (remaining <= 0) return;
    }
  }

  void deliver(Map<String, Object> row) throws Exception {
    long v = number(row, "voucher_id"),
        o = number(row, "aggregate_id"),
        epoch = number(row, "epoch");
    String id = str(row, "event_id");
    JSONObject p = JSON.parseObject(str(row, "payload"));
    switch (str(row, "event_type")) {
      case "REMINDER" -> facade.notifyOpening(v, p.getString("beginTime"), catalog);
      case "CREATE" -> {
        p.put("eventId", id);
        kafka.send("seckill-order-v2", "" + v, JSON.toJSONString(p)).get(10, TimeUnit.SECONDS);
      }
      case "REDIS" -> {
        var current = s.voucher(v, false);
        if (epoch < number(current, "admission_epoch")) return;
        if (!"OPEN".equals(str(current, "admission_state")))
          throw new IllegalStateException("REBUILDING");
        try {
          redis.project(
              v,
              epoch,
              number(row, "sequence_no"),
              id,
              p.getString("kind"),
              p.getLongValue("userId"),
              p.getString("orderId"),
              p.getLongValue("delta"));
        } catch (RuntimeException e) {
          String reason = String.valueOf(e.getMessage());
          if (reason.contains("SEQUENCE_GAP"))
            metrics.counter("seckill_v2_projection_gap").increment();
          if (reason.contains("MISSING_RESERVATION")
              || reason.contains("CORRUPT")
              || reason.contains("WRONGTYPE")) recovery.recover(v);
          throw e;
        }
      }
      case "REFILL" -> { // Projection must catch up before allocating restored stock.
        var voucher = s.voucher(v, false);
        if (redis.inspect(v).sequence() < number(voucher, "projection_seq"))
          throw new IllegalStateException("PROJECTION_PENDING");
        if (facade.refill(v, p.getLongValue("excludedUserId")))
          throw new IllegalStateException("MORE_CANDIDATES");
      }
      case "SUCCESS", "CANCEL_STATS" -> {
        tx.in(
            () -> {
              s.voucher(v, true);
              if (s.one(
                      "SELECT event_id FROM tb_seckill_notification WHERE voucher_id=? AND"
                          + " event_id=?",
                      v,
                      id)
                  == null)
                s.jdbc.update(
                    "INSERT INTO"
                        + " tb_seckill_notification(event_id,voucher_id,user_id,order_id,payload)"
                        + " VALUES(?,?,?,?,?)",
                    id,
                    v,
                    p.getLongValue("userId"),
                    o,
                    str(row, "payload"));
              return null;
            });
        redis.projectBuyerStats(
            p.getLongValue("shopId"),
            p.getString("purchaseDate"),
            p.getLongValue("userId"),
            "" + o,
            id,
            "SUCCESS".equals(str(row, "event_type")) ? "SUCCESS" : "CANCEL");
      }

      default -> throw new IllegalStateException("UNKNOWN_EVENT_TYPE");
    }
  }
}
