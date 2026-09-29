package org.javaup.seckill;

import static org.javaup.seckill.SeckillStore.*;

import java.time.*;
import java.util.*;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.springframework.stereotype.Service;

/** Persisted freeze/epoch protocol. No Redis calls inside the database transaction. */
@Service
public class SeckillRecovery {
  private final SeckillStore s;
  private final SeckillTransactions tx;
  private final RedisAdmissionGateway redis;
  private final SeckillVoucherBloom bloom;

  @org.springframework.beans.factory.annotation.Value("${seckill.v2.activity-zone:Asia/Shanghai}")
  private String activityZone = "Asia/Shanghai";

  public SeckillRecovery(
      SeckillStore s, SeckillTransactions tx, RedisAdmissionGateway redis, SeckillVoucherBloom bloom) {
    this.s = s;
    this.tx = tx;
    this.redis = redis;
    this.bloom = bloom;
  }

  public void recover(long v) {
    if (!redis.available()) throw new SeckillFailure("REDIS_UNAVAILABLE", 503);
    String owner = UUID.randomUUID().toString();
    Map<String, Object> snapshot =
        tx.in(
            () -> {
              var row = s.voucher(v, true);
              var task = s.one("SELECT * FROM tb_seckill_recovery WHERE voucher_id=?", v);
              var now = s.now(v);
              if (task != null
                  && "RETRY".equals(str(task, "status"))
                  && time(task, "next_attempt_at") != null
                  && time(task, "next_attempt_at").isAfter(now)) return null;
              if (task != null
                  && "RUNNING".equals(str(task, "status"))
                  && time(task, "lease_until") != null
                  && time(task, "lease_until").isAfter(now)) return null;
              if ("PAUSED".equals(str(row, "admission_state"))
                  && task != null
                  && "INVARIANT".equals(str(task, "phase"))) return null;
              // A new epoch is used on every takeover; a stale worker can never activate its
              // namespace.
              require(
                  s.jdbc.update(
                      "UPDATE tb_seckill_voucher SET"
                          + " admission_state='REBUILDING',admission_epoch=admission_epoch+1 WHERE"
                          + " voucher_id=?",
                      v));
              long epoch = number(s.voucher(v, false), "admission_epoch");
              if (task == null)
                s.jdbc.update(
                    "INSERT INTO"
                        + " tb_seckill_recovery(voucher_id,epoch,status,lease_owner,lease_until,phase)"
                        + " VALUES(?,?,'RUNNING',?,?,'SNAPSHOT')",
                    v,
                    epoch,
                    owner,
                    now.plusSeconds(30));
              else
                require(
                    s.jdbc.update(
                        "UPDATE tb_seckill_recovery SET"
                            + " epoch=?,status='RUNNING',lease_owner=?,lease_until=?,phase='SNAPSHOT',last_error=NULL,update_time=CURRENT_TIMESTAMP"
                            + " WHERE voucher_id=?",
                        epoch,
                        owner,
                        now.plusSeconds(30),
                        v));
              return s.voucher(v, false);
            });
    if (snapshot == null) return;
    long epoch = number(snapshot, "admission_epoch");
    boolean consistent =
        tx.in(
            () -> {
              var row = s.voucher(v, true);
              if (number(row, "admission_epoch") != epoch) return false;
              if (databaseConsistent(v, row)) return true;
              s.jdbc.update(
                  "UPDATE tb_seckill_voucher SET admission_state='PAUSED' WHERE voucher_id=?", v);
              s.jdbc.update(
                  "UPDATE tb_seckill_recovery SET"
                      + " status='BLOCKED',phase='INVARIANT',lease_until=NULL,last_error='Database"
                      + " mismatch before recovery' WHERE voucher_id=? AND epoch=?",
                  v,
                  epoch);
              return false;
            });
    if (!consistent) throw new SeckillFailure("INVENTORY_INVARIANT", 503);
    try {
      // No external IO within the database transaction; index before reopening admission.
      bloom.register(v);
      Inventory i = inventory(snapshot);
      var rows =
          s.jdbc.queryForList(
              "SELECT * FROM tb_seckill_request WHERE voucher_id=? AND status IN"
                  + " ('PROCESSING','SUCCEEDED')",
              v);
      var bindings = new ArrayList<Reservation>();
      for (var r : rows)
        bindings.add(
            new Reservation(
                str(r, "request_id"),
                str(r, "id"),
                v,
                number(r, "user_id"),
                epoch,
                number(snapshot, "rule_version"),
                time(r, "create_time").atZone(ZoneId.of(activityZone)).toInstant().toEpochMilli(),
                "PROCESSING".equals(str(r, "status")) ? "ACCEPTED" : "COMMITTED"));
      var base = s.one("SELECT status FROM tb_voucher WHERE id=?", v);
      redis.rebuild(
          v,
          epoch,
          i.available(),
          number(snapshot, "rule_version"),
          str(base, "status"),
          time(snapshot, "begin_time").atZone(ZoneId.of(activityZone)).toInstant().toEpochMilli(),
          time(snapshot, "end_time").atZone(ZoneId.of(activityZone)).toInstant().toEpochMilli(),
          number(snapshot, "projection_seq"),
          bindings);
      Boolean ready =
          tx.in(
              () -> {
                var row = s.voucher(v, true);
                var task = s.one("SELECT * FROM tb_seckill_recovery WHERE voucher_id=?", v);
                if (number(row, "admission_epoch") != epoch
                    || !owner.equals(str(task, "lease_owner"))) return false;
                s.jdbc.update(
                    "UPDATE tb_seckill_request SET epoch=? WHERE voucher_id=? AND status IN"
                        + " ('PROCESSING','SUCCEEDED')",
                    epoch,
                    v);
                require(
                    s.jdbc.update(
                        "UPDATE tb_seckill_voucher SET admission_state='OPEN' WHERE voucher_id=?"
                            + " AND admission_epoch=?",
                        v,
                        epoch));
                require(
                    s.jdbc.update(
                        "UPDATE tb_seckill_recovery SET"
                            + " phase='ACTIVATE',lease_until=?,update_time=CURRENT_TIMESTAMP WHERE"
                            + " voucher_id=? AND lease_owner=?",
                        s.now(v).plusSeconds(30),
                        v,
                        owner));
                return true;
              });
      if (!Boolean.TRUE.equals(ready)) return;
      // Recheck the durable fencing token before activation; projection events carry this epoch.
      if (number(s.voucher(v, false), "admission_epoch") != epoch) return;
      redis.activate(v, epoch);
      s.jdbc.update(
          "UPDATE tb_seckill_recovery SET"
              + " status='DONE',phase='DONE',lease_until=NULL,update_time=CURRENT_TIMESTAMP WHERE"
              + " voucher_id=? AND epoch=? AND lease_owner=?",
          v,
          epoch,
          owner);
    } catch (RuntimeException e) {
      s.jdbc.update(
          "UPDATE tb_seckill_recovery SET"
              + " status='RETRY',last_error=?,lease_until=NULL,next_attempt_at=?,update_time=CURRENT_TIMESTAMP"
              + " WHERE voucher_id=? AND epoch=? AND lease_owner=?",
          e.toString(),
          s.now(v).plusSeconds(5),
          v,
          epoch,
          owner);
      throw e;
    }
  }

  private boolean databaseConsistent(long v, Map<String, Object> row) {
    boolean valid;
    try {
      var i = inventory(row);
      long pending =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_seckill_request WHERE voucher_id=? AND"
                  + " status='PROCESSING'",
              Long.class,
              v);
      long sold =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=? AND status=1",
              Long.class,
              v);
      long active =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_seckill_active_purchase WHERE voucher_id=?", Long.class, v);
      long bad =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_seckill_active_purchase a LEFT JOIN"
                  + " tb_seckill_request r ON a.voucher_id=r.voucher_id AND"
                  + " a.order_id=r.id WHERE a.voucher_id=? AND (r.id IS NULL OR"
                  + " a.user_id<>r.user_id OR"
                  + " r.status NOT IN ('PROCESSING','SUCCEEDED'))",
              Long.class,
              v);
      long missing =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_seckill_request r LEFT JOIN tb_seckill_outbox e ON"
                  + " r.voucher_id=e.voucher_id AND e.event_id=CONCAT('create:',r.id)"
                  + " WHERE r.voucher_id=? AND r.status='PROCESSING' AND e.event_id IS"
                  + " NULL",
              Long.class,
              v);
      long mismatchedOrders =
          s.jdbc.queryForObject(
              "SELECT COUNT(*) FROM tb_voucher_order o LEFT JOIN tb_seckill_request r ON"
                  + " o.voucher_id=r.voucher_id AND o.id=r.id WHERE o.voucher_id=? AND"
                  + " o.status=1 AND (r.id IS NULL OR r.status<>'SUCCEEDED' OR"
                  + " r.user_id<>o.user_id)",
              Long.class,
              v);
      valid =
          i.reserved() == pending
              && i.sold() == sold
              && active == pending + sold
              && bad == 0
              && missing == 0
              && mismatchedOrders == 0;
    } catch (IllegalStateException e) {
      valid = false;
    }
    return valid;
  }

  public void audit(long v) {
    Boolean healthy =
        tx.in(
            () -> {
              var row = s.voucher(v, true);
              if (!"OPEN".equals(str(row, "admission_state"))) return null;
              boolean valid = databaseConsistent(v, row);
              if (!valid) {
                s.jdbc.update(
                    "UPDATE tb_seckill_voucher SET admission_state='PAUSED' WHERE voucher_id=?", v);
                var task =
                    s.one("SELECT voucher_id FROM tb_seckill_recovery WHERE voucher_id=?", v);
                if (task == null)
                  s.jdbc.update(
                      "INSERT INTO tb_seckill_recovery(voucher_id,epoch,status,phase,last_error)"
                          + " VALUES(?,?,'BLOCKED','INVARIANT','Database inventory mismatch')",
                      v,
                      number(row, "admission_epoch"));
                else
                  s.jdbc.update(
                      "UPDATE tb_seckill_recovery SET"
                          + " status='BLOCKED',phase='INVARIANT',last_error='Database inventory"
                          + " mismatch' WHERE voucher_id=?",
                      v);
                return false;
              }
              return true;
            });
    if (Boolean.FALSE.equals(healthy)) throw new SeckillFailure("INVENTORY_INVARIANT", 503);
    if (Boolean.TRUE.equals(healthy)) {
      var row = s.voucher(v, false);
      try {
        var state = redis.inspect(v);
        if (state.epoch() != number(row, "admission_epoch")
            || state.sequence() > number(row, "projection_seq")) recover(v);
        else if ("BUSY"
            .equals(
                redis.auditProjection(
                    v,
                    number(row, "admission_epoch"),
                    number(row, "projection_seq"),
                    number(row, "init_stock")))) {
          if ("BUSY"
              .equals(
                  redis.auditProjectionPaged(
                      v,
                      number(row, "admission_epoch"),
                      number(row, "projection_seq"),
                      number(row, "init_stock"))))
            throw new SeckillFailure("AUDIT_CONCURRENT_RETRY", 503);
        }
      } catch (RuntimeException failure) {
        if (failure instanceof SeckillFailure f && "AUDIT_CONCURRENT_RETRY".equals(f.getCode()))
          throw failure;
        if (redis.available()) recover(v);
        else throw failure;
      }
    }
  }
}
