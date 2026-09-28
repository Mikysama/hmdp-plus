package org.javaup.seckill;

import java.util.*;
import java.util.concurrent.Semaphore;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.javaup.service.IUserInfoService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.springframework.stereotype.Service;

@Service
public class SeckillFacade {
  private final RedisAdmissionGateway redis;
  private final SeckillTransactions tx;
  private final SeckillStore store;
  private final IUserInfoService users;
  private final SnowflakeIdGenerator ids;
  private final Semaphore admission;
  private final SeckillAdmissionCache cache;
  private final SeckillAdmissionPublisher publisher;

  public SeckillFacade(
      RedisAdmissionGateway redis,
      SeckillTransactions tx,
      SeckillStore store,
      IUserInfoService users,
      SnowflakeIdGenerator ids,
      SeckillAdmissionCache cache,
      SeckillAdmissionPublisher publisher,
      @org.springframework.beans.factory.annotation.Value(
              "${seckill.v2.local-admission-concurrency:128}")
          int concurrency) {
    this.redis = redis;
    this.tx = tx;
    this.store = store;
    this.users = users;
    this.ids = ids;
    this.cache = cache;
    this.publisher = publisher;
    this.admission = new Semaphore(concurrency);
  }

  public SeckillResult submit(long v, long u, String request, String token, boolean auto) {
    if (request == null || !request.matches("[A-Za-z0-9_-]{1,64}"))
      throw new SeckillFailure("INVALID_REQUEST_ID", 400);
    if (!admission.tryAcquire()) throw new SeckillFailure("ADMISSION_BUSY", 429);
    Reservation r = null;
    String attempt = UUID.randomUUID().toString();
    boolean entered = false;
    try {
      try {
        cache.check(v, u, request);
        entered = redis.tryEnter(v, attempt);
      } catch (RuntimeException e) {
        throw redisFailure(e);
      }
      if (!entered) throw new SeckillFailure("ADMISSION_BUSY", 429);
      // The whole HTTP submission path is database-free, including successful reservations.
      try {
        r = redis.reserve(v, u, request, token, "" + ids.nextId(), auto);
      } catch (RuntimeException e) {
        throw redisFailure(e);
      }
      publisher.publish(r, auto);
      return new SeckillResult(r.requestId(), r.orderId(), "QUEUED", null, null);
    } finally {
      if (r != null)
        try {
          redis.releaseSlot(v, r);
        } catch (RuntimeException ignored) {
          /* bounded lease, never release business stock */
        }
      if (entered)
        try {
          redis.leave(v, attempt);
        } catch (RuntimeException ignored) {
          /* bounded call lease */
        }
      admission.release();
    }
  }

  public static SeckillFailure redisFailure(RuntimeException e) {
    if (e instanceof SeckillFailure failure) return failure;
    String m = String.valueOf(e.getMessage());
    for (String code :
        List.of(
            "SOLD_OUT",
            "ALREADY_PURCHASED",
            "USER_OCCUPIED",
            "NOT_STARTED",
            "ENDED",
            "VOUCHER_UNAVAILABLE",
            "TOKEN_INVALID",
            "TOKEN_BOUND",
            "AUTO_ISSUE_MISMATCH",
            "INVALID_TOKEN",
            "ALREADY_RESERVED",
            "INACTIVE")) if (m.contains(code)) return new SeckillFailure(code, 409);
    if (m.contains("RATE") || m.contains("BUSY") || m.contains("INFLIGHT"))
      return new SeckillFailure("RATE_LIMITED", 429);
    return new SeckillFailure("ADMISSION_UNAVAILABLE", 503);
  }

  public SeckillResult result(long voucher, long user, String request) {
    var persisted = tx.result(voucher, user, request);
    if (!"NOT_FOUND".equals(persisted.status())) return persisted;
    try {
      var pending = redis.findReservation(voucher, user, request);
      if (pending != null)
        return new SeckillResult(request, pending.orderId(), "PENDING", null, null);
    } catch (RuntimeException unavailable) {
      // The committed database result is still authoritative. Missing Redis is not failure.
    }
    return persisted;
  }

  public void notifyOpening(long voucherId, String beginTime, SeckillCatalogService catalog) {
    long after = 0;
    var begin = java.time.LocalDateTime.parse(beginTime);
    while (true) {
      var page = catalog.reminderSubscribers(voucherId, after, 100);
      if (page.isEmpty()) return;
      for (long userId : page) {
        var user = users.getByUserId(userId);
        catalog.recordReminder(voucherId, userId, begin, user == null ? null : user.getLevel());
      }
      after = page.get(page.size() - 1);
    }
  }

  public boolean refill(long v, long exclude) {
    var candidates =
        store.jdbc.queryForList(
            "SELECT * FROM tb_seckill_subscription WHERE voucher_id=? AND status='WAITING' AND"
                + " user_id<>? ORDER BY subscribed_at,user_id LIMIT 100",
            v,
            exclude);
    boolean retry = false;
    for (var c : candidates) {
      long u = SeckillStore.number(c, "user_id");
      if (u == exclude) continue;
      var user = users.getByUserId(u);
      if (tx.invalidateCandidate(
          v, u, SeckillStore.number(c, "version"), user == null ? null : user.getLevel())) continue;
      String request = "auto_" + u + "_" + SeckillStore.number(c, "version");
      try {
        submit(v, u, request, redis.issueToken(v, u), true);
      } catch (SeckillFailure e) {
        if ("ALREADY_RESERVED".equals(e.getCode())) {
          if (!tx.hasActivePurchase(v, u)) retry = true;
          continue;
        }
        if (List.of("SOLD_OUT", "ENDED", "VOUCHER_UNAVAILABLE").contains(e.getCode())) return false;
        throw e;
      }
    }
    return retry || candidates.size() == 100;
  }
}
