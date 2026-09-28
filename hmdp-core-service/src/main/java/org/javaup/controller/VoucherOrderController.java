package org.javaup.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.javaup.dto.*;
import org.javaup.execute.RateLimitHandler;
import org.javaup.ratelimit.extension.RateLimitScene;
import org.javaup.seckill.*;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.utils.UserHolder;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
  private final SeckillFacade facade;
  private final SeckillTransactions tx;
  private final RedisAdmissionGateway redis;
  private final SeckillStore store;
  private final RateLimitHandler limit;
  private final SeckillRecovery recovery;

  public VoucherOrderController(
      SeckillFacade facade,
      SeckillTransactions tx,
      RedisAdmissionGateway redis,
      SeckillStore store,
      RateLimitHandler limit,
      SeckillRecovery recovery) {
    this.facade = facade;
    this.tx = tx;
    this.redis = redis;
    this.store = store;
    this.limit = limit;
    this.recovery = recovery;
  }

  public record Submit(
      @NotBlank @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String requestId,
      @NotBlank String accessToken) {}

  public record Cancel(@NotNull @Positive Long voucherId, @NotNull @Positive Long orderId) {}

  long user() {
    return UserHolder.getUser().getId();
  }

  @GetMapping("/seckill/token/{id}")
  public Result<String> token(@PathVariable Long id) {
    limit.execute(id, user(), RateLimitScene.ISSUE_TOKEN);
    try {
      return Result.ok(redis.issueToken(id, user()));
    } catch (RuntimeException e) {
      throw SeckillFacade.redisFailure(e);
    }
  }

  @PostMapping("/seckill/{id}")
  public Result<SeckillResult> submit(@PathVariable Long id, @Valid @RequestBody Submit body) {
    limit.execute(id, user(), RateLimitScene.SECKILL_ORDER);
    return Result.ok(facade.submit(id, user(), body.requestId(), body.accessToken(), false));
  }

  @GetMapping("/seckill/result")
  public Result<SeckillResult> result(
      @RequestParam Long voucherId, @RequestParam String requestId) {
    limit.execute(voucherId, user(), RateLimitScene.RESULT_QUERY);
    return Result.ok(facade.result(voucherId, user(), requestId));
  }

  @PostMapping("/cancel")
  public Result<SeckillResult> cancel(@Valid @RequestBody Cancel body) {
    return Result.ok(tx.cancel(body.voucherId(), user(), body.orderId()));
  }

  @PostMapping("/get/seckill/voucher/order-id/by/voucher-id")
  public Result<String> owned(@Valid @RequestBody GetVoucherOrderByVoucherIdDto body) {
    limit.execute(body.getVoucherId(), user(), RateLimitScene.RESULT_QUERY);
    var row =
        store.one(
            "SELECT id FROM tb_voucher_order WHERE voucher_id=? AND user_id=? AND status=1",
            body.getVoucherId(),
            user());
    return Result.ok(row == null ? null : SeckillStore.str(row, "id"));
  }

  @GetMapping("/notifications")
  public Result<java.util.List<java.util.Map<String, Object>>> notifications(
      @RequestParam Long voucherId, @RequestParam(defaultValue = "") String after) {
    limit.execute(voucherId, user(), RateLimitScene.RESULT_QUERY);
    return Result.ok(
        store.jdbc.queryForList(
            "SELECT event_id,payload,create_time FROM tb_seckill_notification WHERE voucher_id=?"
                + " AND user_id=? AND event_id>? ORDER BY event_id LIMIT 100",
            voucherId,
            user(),
            after));
  }

  @PostMapping("/reconciliation/task/all")
  public Result<Void> audit() {
    long after = 0;
    while (true) {
      var page = store.vouchers(after, 100);
      if (page.isEmpty()) break;
      for (long v : page) recovery.audit(v);
      after = page.get(page.size() - 1);
    }
    return Result.ok();
  }
}
