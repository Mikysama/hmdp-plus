package org.javaup.seckill;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER;

import io.micrometer.core.instrument.MeterRegistry;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.springframework.stereotype.Component;

/** Existence hint only. Neither a positive result nor fallback grants purchase eligibility. */
@Component
public class SeckillVoucherBloom {
  private final BloomFilterHandlerFactory filters;
  private final RedisAdmissionGateway redis;
  private final MeterRegistry metrics;

  public SeckillVoucherBloom(
      BloomFilterHandlerFactory filters, RedisAdmissionGateway redis, MeterRegistry metrics) {
    this.filters = filters;
    this.redis = redis;
    this.metrics = metrics;
  }

  public void check(long voucher) {
    if (voucher <= 0) throw new SeckillFailure("VOUCHER_UNAVAILABLE", 409);
    final boolean maybePresent;
    try {
      maybePresent = filters.get(BLOOM_FILTER_HANDLER_VOUCHER).contains(Long.toString(voucher));
    } catch (RuntimeException unavailable) {
      // Missing configuration / Redis failures must not manufacture a definitive NOT_FOUND.
      // The original Redis-only admission path will still fail closed if Redis is unavailable.
      metrics.counter("seckill_v2_bloom_fallback", "reason", "unavailable").increment();
      return;
    }
    if (maybePresent) return;
    // A shared filter may be lost or partially populated. A valid admission pointer takes
    // precedence over a Bloom negative. This one-key probe never falls back to SQL.
    if (redis.hasAdmission(voucher)) {
      metrics.counter("seckill_v2_bloom_fallback", "reason", "existing_admission").increment();
      try {
        register(voucher);
      } catch (SeckillFailure ignored) {
        // Best-effort repair; Lua still validates the actual activity, lease and stock.
      }
      return;
    }
    metrics.counter("seckill_v2_bloom_rejected").increment();
    throw new SeckillFailure("VOUCHER_UNAVAILABLE", 409);
  }

  /** Call outside SQL transactions, before creation/activation. False positives are safe. */
  public void register(long voucher) {
    try {
      filters.get(BLOOM_FILTER_HANDLER_VOUCHER).ensureInitializedAndAdd(Long.toString(voucher));
    } catch (RuntimeException unavailable) {
      metrics.counter("seckill_v2_bloom_registration_failed").increment();
      throw new SeckillFailure("BLOOM_UNAVAILABLE", 503);
    }
  }
}
