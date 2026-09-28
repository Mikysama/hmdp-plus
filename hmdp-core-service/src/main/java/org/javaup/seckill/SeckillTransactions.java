package org.javaup.seckill;

import static org.javaup.seckill.SeckillStore.*;

import com.alibaba.fastjson.JSON;
import java.time.*;
import java.util.*;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** The only writer of V2 business state; no Redis or MQ call is allowed here. */
@Service
public class SeckillTransactions {
  final SeckillStore s;
  final TransactionTemplate transaction;
  final int lifetime;

  @Autowired
  public SeckillTransactions(
      SeckillStore s,
      PlatformTransactionManager manager,
      @Value("${seckill.v2.request-seconds:60}") int lifetime) {
    this(s, new TransactionTemplate(manager), lifetime);
  }

  public SeckillTransactions(SeckillStore s, TransactionTemplate transaction, int lifetime) {
    this.s = s;
    this.transaction = transaction;
    this.lifetime = lifetime;
    this.transaction.setTimeout(10);
  }

  public <T> T in(java.util.function.Supplier<T> action) {
    return transaction.execute(status -> action.get());
  }

  void open(Map<String, Object> v) {
    if (!"OPEN".equals(str(v, "admission_state")))
      throw new SeckillFailure("ADMISSION_PAUSED", 503);
  }

  public SeckillResult result(long v, long u, String r) {
    var row = s.request(v, u, r);
    return row == null
        ? new SeckillResult(r, null, "NOT_FOUND", null, null)
        : SeckillStore.result(row);
  }

  @Value("${seckill.v2.activity-zone:Asia/Shanghai}")
  private String activityZone = "Asia/Shanghai";

  /** Kafka consumer boundary: acceptance and order creation commit or roll back together. */
  public SeckillResult fulfill(Reservation r, Integer level, boolean auto, int queueSeconds) {
    return in(
        () -> {
          var voucher = s.voucher(r.voucherId(), true);
          open(voucher);
          var existing = s.request(r.voucherId(), r.userId(), r.requestId());
          if (existing != null) {
            if (!r.orderId().equals(str(existing, "id"))
                && r.epoch() == number(voucher, "admission_epoch"))
              projection(
                  voucher,
                  r.voucherId(),
                  Long.parseLong(r.orderId()),
                  "RELEASE",
                  r.userId(),
                  1,
                  "extra:" + r.orderId());
            return "PROCESSING".equals(str(existing, "status"))
                ? complete(r.voucherId(), number(existing, "id"))
                : SeckillStore.result(existing);
          }
          var now = s.now(r.voucherId());
          long nowMillis = now.atZone(java.time.ZoneId.of(activityZone)).toInstant().toEpochMilli();
          String reason =
              r.epoch() != number(voucher, "admission_epoch")
                  ? "STALE_EPOCH"
                  : nowMillis >= r.createdAt() + queueSeconds * 1000L ? "QUEUE_EXPIRED" : null;
          if (reason != null) {
            insertRequest(r, "FAILED", reason, auto, now);
            if (r.epoch() == number(voucher, "admission_epoch"))
              projection(
                  voucher,
                  r.voucherId(),
                  Long.parseLong(r.orderId()),
                  "RELEASE",
                  r.userId(),
                  1,
                  "queue-fail:" + r.orderId());
            failQueuedSubscription(r, auto);
            return result(r.voucherId(), r.userId(), r.requestId());
          }
          // Rules are checked in this same locked transaction, using the current rule version.
          var current =
              new Reservation(
                  r.requestId(),
                  r.orderId(),
                  r.voucherId(),
                  r.userId(),
                  r.epoch(),
                  number(voucher, "rule_version"),
                  r.createdAt(),
                  r.state());
          var accepted = accept(current, level, auto, false);
          return "PROCESSING".equals(accepted.status())
              ? complete(r.voucherId(), Long.parseLong(accepted.orderId()))
              : accepted;
        });
  }

  private void failQueuedSubscription(Reservation r, boolean auto) {
    if (!auto) return;
    var sub =
        s.one(
            "SELECT * FROM tb_seckill_subscription WHERE voucher_id=? AND user_id=?",
            r.voucherId(),
            r.userId());
    if (sub != null
        && "WAITING".equals(str(sub, "status"))
        && r.requestId().equals("auto_" + r.userId() + "_" + number(sub, "version")))
      s.jdbc.update(
          "UPDATE tb_seckill_subscription SET status='FAILED',version=version+1"
              + " WHERE voucher_id=? AND user_id=? AND status='WAITING' AND version=?",
          r.voucherId(),
          r.userId(),
          number(sub, "version"));
  }

  /** Legacy V2 drain/test entry. New HTTP requests only publish to Kafka. */
  public SeckillResult accept(Reservation r, Integer level, boolean auto) {
    return accept(r, level, auto, true);
  }

  private SeckillResult accept(Reservation r, Integer level, boolean auto, boolean createEvent) {
    return in(
        () -> {
          long v = r.voucherId(), u = r.userId(), o = Long.parseLong(r.orderId());
          var voucher = s.voucher(v, true);
          open(voucher);
          var existing = s.request(v, u, r.requestId());
          if (existing != null) {
            if (!r.orderId().equals(str(existing, "id"))
                && r.epoch() == number(voucher, "admission_epoch"))
              projection(voucher, v, o, "RELEASE", u, 1, "extra:" + r.orderId());
            return SeckillStore.result(existing);
          }
          if (r.epoch() != number(voucher, "admission_epoch"))
            throw new SeckillFailure("STALE_EPOCH", 503);
          if (r.ruleVersion() != number(voucher, "rule_version"))
            throw new SeckillFailure("RULE_VERSION_CHANGED", 503);
          LocalDateTime now = s.now(v);
          String reason = null;
          var base = s.one("SELECT status FROM tb_voucher WHERE id=?", v);
          if (base == null || number(base, "status") != 1) reason = "VOUCHER_UNAVAILABLE";
          else if (now.isBefore(time(voucher, "begin_time"))) reason = "NOT_STARTED";
          else if (!now.isBefore(time(voucher, "end_time"))) reason = "ENDED";
          else if (!eligible(voucher, level)) reason = "LEVEL_NOT_ALLOWED";
          else if (s.one(
                  "SELECT order_id FROM tb_seckill_active_purchase WHERE voucher_id=? AND"
                      + " user_id=?",
                  v,
                  u)
              != null) reason = "ALREADY_PURCHASED";
          else if (inventory(voucher).available() == 0) reason = "SOLD_OUT";
          var sub =
              auto
                  ? s.one(
                      "SELECT * FROM tb_seckill_subscription WHERE voucher_id=? AND user_id=?",
                      v,
                      u)
                  : null;
          boolean subscriptionMatches =
              !auto
                  || (sub != null
                      && "WAITING".equals(str(sub, "status"))
                      && r.requestId().equals("auto_" + u + "_" + number(sub, "version")));
          if (!subscriptionMatches) reason = "SUBSCRIPTION_CHANGED";
          insertRequest(
              r, reason == null ? "PROCESSING" : "FAILED", reason, auto, now.plusSeconds(lifetime));
          if (reason != null) {
            if (auto && subscriptionMatches)
              s.jdbc.update(
                  "UPDATE tb_seckill_subscription SET status='FAILED',version=version+1 WHERE"
                      + " voucher_id=? AND user_id=? AND status='WAITING'",
                  v,
                  u);
            projection(voucher, v, o, "RELEASE", u, 1, "reject:" + o);
            return result(v, u, r.requestId());
          }
          Inventory before = inventory(voucher), after = before.reserve();
          s.saveInventory(v, after);
          require(
              s.jdbc.update(
                  "INSERT INTO"
                      + " tb_seckill_active_purchase(voucher_id,user_id,order_id)"
                      + " VALUES(?,?,?)",
                  v,
                  u,
                  o));
          if (auto)
            require(
                s.jdbc.update(
                    "UPDATE tb_seckill_subscription SET"
                        + " status='ASSIGNED',order_id=?,version=version+1 WHERE voucher_id=? AND"
                        + " user_id=? AND status='WAITING'",
                    o,
                    v,
                    u));
          operation(v, o, "reserve:" + o, "RESERVE", before, after, null);
          projection(voucher, v, o, "ACCEPT", u, 0, "accept:" + o);
          if (createEvent)
            event(
                v,
                o,
                "CREATE",
                number(voucher, "admission_epoch"),
                null,
                "create:" + o,
                Map.of(
                    "voucherId",
                    "" + v,
                    "orderId",
                    "" + o,
                    "userId",
                    "" + u,
                    "requestId",
                    r.requestId(),
                    "schemaVersion",
                    2));
          return result(v, u, r.requestId());
        });
  }

  public static boolean eligible(Map<String, Object> v, Integer level) {
    String allowed = str(v, "allowed_levels");
    Object min = v.get("min_level");
    if (min != null
        && (!(min instanceof Number)
            || ((Number) min).intValue() < 0
            || ((Number) min).intValue() > 10)) return false;
    if ((allowed == null || allowed.isBlank()) && min == null) return true;
    if (level == null) return false;
    if (allowed != null && !allowed.isBlank()) {
      try {
        var levels = new HashSet<Integer>();
        for (String part : allowed.split(",", -1)) {
          int n = Integer.parseInt(part.trim());
          if (n < 0 || n > 10) return false;
          levels.add(n);
        }
        if (!levels.contains(level)) return false;
      } catch (RuntimeException ex) {
        return false;
      }
    }
    return min == null || level >= ((Number) min).intValue();
  }

  void insertRequest(
      Reservation r, String state, String reason, boolean auto, LocalDateTime expires) {
    require(
        s.jdbc.update(
            "INSERT INTO"
                + " tb_seckill_request(id,voucher_id,user_id,request_id,epoch,status,expires_at,reason_code,auto_issue)"
                + " VALUES(?,?,?,?,?,?,?,?,?)",
            Long.parseLong(r.orderId()),
            r.voucherId(),
            r.userId(),
            r.requestId(),
            r.epoch(),
            state,
            expires,
            reason,
            auto));
  }

  public SeckillResult completeEvent(long v, long o, long u, String requestId) {
    return in(
        () -> {
          s.voucher(v, true);
          var request = s.orderRequest(v, o);
          if (request == null
              || number(request, "user_id") != u
              || !requestId.equals(str(request, "request_id")))
            throw new SeckillFailure("EVENT_IDENTITY_MISMATCH", 409);
          return complete(v, o);
        });
  }

  public SeckillResult complete(long v, long o) {
    return in(
        () -> {
          var voucher = s.voucher(v, true);
          open(voucher);
          var request = s.orderRequest(v, o);
          if (request == null) throw new SeckillFailure("REQUEST_NOT_FOUND", 404);
          if (!"PROCESSING".equals(str(request, "status"))) return SeckillStore.result(request);
          if (!s.now(v).isBefore(time(request, "expires_at")))
            return fail(voucher, request, "EXPIRED");
          long u = number(request, "user_id");
          Inventory before = inventory(voucher), after = before.commit();
          require(
              s.jdbc.update(
                  "INSERT INTO"
                      + " tb_voucher_order(id,user_id,voucher_id,status,create_time,update_time)"
                      + " VALUES(?,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                  o,
                  u,
                  v));
          s.saveInventory(v, after);
          require(
              s.jdbc.update(
                  "UPDATE tb_seckill_request SET"
                      + " status='SUCCEEDED',version=version+1,update_time=CURRENT_TIMESTAMP WHERE"
                      + " voucher_id=? AND id=? AND status='PROCESSING'",
                  v,
                  o));
          s.jdbc.update(
              "UPDATE tb_seckill_subscription SET status='ASSIGNED',order_id=?,version=version+1"
                  + " WHERE voucher_id=? AND user_id=? AND status='WAITING'",
              o,
              v,
              u);
          operation(v, o, "commit:" + o, "COMMIT", before, after, null);
          projection(voucher, v, o, "COMMIT", u, 0, "commit:" + o);
          event(
              v,
              o,
              "SUCCESS",
              number(voucher, "admission_epoch"),
              null,
              "success:" + o,
              Map.of(
                  "userId",
                  u,
                  "autoIssue",
                  number(request, "auto_issue") != 0,
                  "shopId",
                  number(s.one("SELECT shop_id FROM tb_voucher WHERE id=?", v), "shop_id"),
                  "purchaseDate",
                  time(
                          s.one(
                              "SELECT create_time FROM tb_voucher_order WHERE voucher_id=? AND"
                                  + " id=?",
                              v,
                              o),
                          "create_time")
                      .toLocalDate()
                      .toString()));
          return result(v, u, str(request, "request_id"));
        });
  }

  SeckillResult fail(Map<String, Object> voucher, Map<String, Object> r, String reason) {
    long v = number(r, "voucher_id"), u = number(r, "user_id"), o = number(r, "id");
    Inventory before = inventory(voucher), after = before.release();
    s.saveInventory(v, after);
    require(
        s.jdbc.update(
            "UPDATE tb_seckill_request SET"
                + " status='FAILED',reason_code=?,version=version+1,update_time=CURRENT_TIMESTAMP"
                + " WHERE voucher_id=? AND id=? AND status='PROCESSING'",
            reason,
            v,
            o));
    require(
        s.jdbc.update(
            "DELETE FROM tb_seckill_active_purchase WHERE voucher_id=? AND user_id=? AND"
                + " order_id=?",
            v,
            u,
            o));
    operation(v, o, "fail:" + o, "RELEASE", before, after, null);
    projection(voucher, v, o, "RELEASE", u, 1, "fail:" + o);
    s.jdbc.update(
        "UPDATE tb_seckill_subscription SET status='FAILED',version=version+1 WHERE voucher_id=?"
            + " AND user_id=? AND order_id=?",
        v,
        u,
        o);
    event(
        v,
        o,
        "REFILL",
        number(voucher, "admission_epoch"),
        null,
        "refill-fail:" + o,
        Map.of("excludedUserId", u));
    return result(v, u, str(r, "request_id"));
  }

  public SeckillResult cancel(long v, long u, long o) {
    return in(
        () -> {
          var voucher = s.voucher(v, true);
          open(voucher);
          var r = s.orderRequest(v, o);
          if (r == null || number(r, "user_id") != u)
            throw new SeckillFailure("ORDER_NOT_FOUND", 404);
          if ("CANCELLED".equals(str(r, "status"))) return SeckillStore.result(r);
          if (!"SUCCEEDED".equals(str(r, "status")))
            throw new SeckillFailure("ORDER_NOT_CANCELLABLE", 409);
          require(
              s.jdbc.update(
                  "UPDATE tb_voucher_order SET status=2,update_time=CURRENT_TIMESTAMP WHERE"
                      + " voucher_id=? AND id=? AND user_id=? AND status=1",
                  v,
                  o,
                  u));
          require(
              s.jdbc.update(
                  "UPDATE tb_seckill_request SET"
                      + " status='CANCELLED',version=version+1,update_time=CURRENT_TIMESTAMP WHERE"
                      + " voucher_id=? AND id=? AND status='SUCCEEDED'",
                  v,
                  o));
          require(
              s.jdbc.update(
                  "DELETE FROM tb_seckill_active_purchase WHERE voucher_id=? AND user_id=? AND"
                      + " order_id=?",
                  v,
                  u,
                  o));
          Inventory before = inventory(voucher), after = before.cancel();
          s.saveInventory(v, after);
          operation(v, o, "cancel:" + o, "CANCEL", before, after, null);
          s.jdbc.update(
              "UPDATE tb_seckill_subscription SET status='CANCELLED',version=version+1 WHERE"
                  + " voucher_id=? AND user_id=? AND order_id=? AND status='ASSIGNED'",
              v,
              u,
              o);
          projection(voucher, v, o, "CANCEL", u, 1, "cancel:" + o);
          event(
              v,
              o,
              "REFILL",
              number(voucher, "admission_epoch"),
              null,
              "refill-cancel:" + o,
              Map.of("excludedUserId", u));
          event(
              v,
              o,
              "CANCEL_STATS",
              number(voucher, "admission_epoch"),
              null,
              "stats-cancel:" + o,
              Map.of(
                  "userId",
                  u,
                  "shopId",
                  number(s.one("SELECT shop_id FROM tb_voucher WHERE id=?", v), "shop_id"),
                  "purchaseDate",
                  time(
                          s.one(
                              "SELECT create_time FROM tb_voucher_order WHERE voucher_id=? AND"
                                  + " id=?",
                              v,
                              o),
                          "create_time")
                      .toLocalDate()
                      .toString()));
          return result(v, u, str(r, "request_id"));
        });
  }

  public void resolveOrphan(Reservation r) {
    in(
        () -> {
          var v = s.voucher(r.voucherId(), true);
          open(v);
          if (r.epoch() != number(v, "admission_epoch")) return null;
          var existing = s.request(r.voucherId(), r.userId(), r.requestId());
          if (existing == null) {
            boolean auto = r.requestId().startsWith("auto_");
            insertRequest(r, "FAILED", "ACCEPTANCE_EXPIRED", auto, s.now(r.voucherId()));
            failQueuedSubscription(r, auto);
            projection(
                v,
                r.voucherId(),
                Long.parseLong(r.orderId()),
                "RELEASE",
                r.userId(),
                1,
                "orphan:" + r.orderId());
          } else if (!r.orderId().equals(str(existing, "id")))
            projection(
                v,
                r.voucherId(),
                Long.parseLong(r.orderId()),
                "RELEASE",
                r.userId(),
                1,
                "extra:" + r.orderId());
          return null;
        });
  }

  public void expire(long v, long o) {
    in(
        () -> {
          var voucher = s.voucher(v, true);
          open(voucher);
          var r = s.orderRequest(v, o);
          if (r != null
              && "PROCESSING".equals(str(r, "status"))
              && !s.now(v).isBefore(time(r, "expires_at"))) fail(voucher, r, "EXPIRED");
          return null;
        });
  }

  public Map<String, Object> adjust(long v, String id, long expected, int total) {
    return in(
        () -> {
          var voucher = s.voucher(v, true);
          var old =
              s.one(
                  "SELECT * FROM tb_seckill_operation WHERE voucher_id=? AND operation_id=?",
                  v,
                  "adjust:" + id);
          String payload = JSON.toJSONString(Map.of("expectedVersion", expected, "total", total));
          if (old != null) {
            if (!JSON.parseObject(payload).equals(JSON.parseObject(str(old, "payload"))))
              throw new SeckillFailure("IDEMPOTENCY_CONFLICT", 409);
            return old;
          }
          open(voucher);
          if (expected != number(voucher, "version"))
            throw new SeckillFailure("VERSION_CONFLICT", 409);
          var before = inventory(voucher);
          Inventory after;
          try {
            after = before.adjust(total);
          } catch (IllegalStateException e) {
            throw new SeckillFailure("QUOTA_BELOW_ALLOCATED", 409);
          }
          s.saveInventory(v, after);
          operation(v, null, "adjust:" + id, "ADJUST", before, after, payload);
          if (total < before.total()) {
            pauseForRebuild(v);
          } else if (total > before.total()) {
            projection(
                voucher, v, 0, "ADJUST", 0, total - before.total(), "adjust:" + v + ":" + id);
            event(
                v,
                0,
                "REFILL",
                number(voucher, "admission_epoch"),
                null,
                "refill-adjust:" + v + ":" + id,
                Map.of("excludedUserId", 0));
          }
          return s.one(
              "SELECT * FROM tb_seckill_operation WHERE voucher_id=? AND operation_id=?",
              v,
              "adjust:" + id);
        });
  }

  public void pauseForRebuild(long v) {
    require(
        s.jdbc.update(
            "UPDATE tb_seckill_voucher SET"
                + " admission_state='REBUILDING',admission_epoch=admission_epoch+1 WHERE"
                + " voucher_id=?",
            v));
  }

  public void subscribe(long v, long u, boolean enabled) {
    in(
        () -> {
          var voucher = s.voucher(v, true);
          open(voucher);
          var sub =
              s.one("SELECT * FROM tb_seckill_subscription WHERE voucher_id=? AND user_id=?", v, u);
          if (sub == null && enabled)
            s.jdbc.update(
                "INSERT INTO tb_seckill_subscription(voucher_id,user_id,subscribed_at,status)"
                    + " VALUES(?,?,CURRENT_TIMESTAMP(3),'WAITING')",
                v,
                u);
          else if (sub != null
              && (!enabled || !List.of("WAITING", "ASSIGNED").contains(str(sub, "status"))))
            s.jdbc.update(
                "UPDATE tb_seckill_subscription SET"
                    + " status=?,subscribed_at=CURRENT_TIMESTAMP(3),order_id=NULL,version=version+1"
                    + " WHERE voucher_id=? AND user_id=?",
                enabled ? "WAITING" : "CANCELLED",
                v,
                u);
          return null;
        });
  }

  public boolean invalidateCandidate(long v, long u, long version, Integer level) {
    return in(
        () -> {
          var voucher = s.voucher(v, true);
          open(voucher);
          if (eligible(voucher, level)) return false;
          s.jdbc.update(
              "UPDATE tb_seckill_subscription SET status='INELIGIBLE',version=version+1 WHERE"
                  + " voucher_id=? AND user_id=? AND status='WAITING' AND version=?",
              v,
              u,
              version);
          return true;
        });
  }

  public boolean hasActivePurchase(long v, long u) {
    return in(
        () -> {
          s.voucher(v, true);
          var active =
              s.one(
                  "SELECT order_id FROM tb_seckill_active_purchase WHERE voucher_id=? AND"
                      + " user_id=?",
                  v,
                  u);
          if (active == null) return false;
          s.jdbc.update(
              "UPDATE tb_seckill_subscription SET status='ASSIGNED',order_id=?,version=version+1"
                  + " WHERE voucher_id=? AND user_id=? AND status='WAITING'",
              number(active, "order_id"),
              v,
              u);
          return true;
        });
  }

  public int subscribeStatus(long v, long u) {
    var active =
        s.one(
            "SELECT order_id FROM tb_seckill_active_purchase WHERE voucher_id=? AND user_id=?",
            v,
            u);
    if (active != null) {
      var request = s.orderRequest(v, number(active, "order_id"));
      if (request != null && "SUCCEEDED".equals(str(request, "status"))) return 2;
      return 1;
    }
    var row =
        s.one("SELECT status FROM tb_seckill_subscription WHERE voucher_id=? AND user_id=?", v, u);
    return row != null && List.of("WAITING", "ASSIGNED").contains(str(row, "status")) ? 1 : 0;
  }

  public void projection(
      Map<String, Object> voucher, long v, long o, String kind, long u, int delta, String op) {
    String eid = "redis:" + op;
    if (s.one("SELECT event_id FROM tb_seckill_outbox WHERE voucher_id=? AND event_id=?", v, eid)
        != null) return;
    require(
        s.jdbc.update(
            "UPDATE tb_seckill_voucher SET projection_seq=projection_seq+1 WHERE voucher_id=?", v));
    long seq = number(s.voucher(v, false), "projection_seq");
    event(
        v,
        o,
        "REDIS",
        number(voucher, "admission_epoch"),
        seq,
        eid,
        Map.of("kind", kind, "userId", u, "orderId", "" + o, "delta", delta));
    if ("RELEASE".equals(kind)) {
      event(
          v,
          o,
          "REFILL",
          number(voucher, "admission_epoch"),
          null,
          "refill-projection:" + op,
          Map.of("excludedUserId", u));
    }
  }

  public void event(
      long v, long o, String kind, long epoch, Long seq, String id, Map<String, Object> payload) {
    require(
        s.jdbc.update(
            "INSERT INTO"
                + " tb_seckill_outbox(event_id,voucher_id,aggregate_id,event_type,epoch,sequence_no,payload)"
                + " VALUES(?,?,?,?,?,?,?)",
            id,
            v,
            o,
            kind,
            epoch,
            seq,
            JSON.toJSONString(payload)));
  }

  void operation(
      long v, Long o, String id, String kind, Inventory before, Inventory after, String payload) {
    require(
        s.jdbc.update(
            "INSERT INTO"
                + " tb_seckill_operation(voucher_id,operation_id,order_id,kind,available_delta,reserved_delta,sold_delta,after_available,after_reserved,after_sold,payload)"
                + " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            v,
            id,
            o,
            kind,
            after.available() - before.available(),
            after.reserved() - before.reserved(),
            after.sold() - before.sold(),
            after.available(),
            after.reserved(),
            after.sold(),
            payload));
  }
}
