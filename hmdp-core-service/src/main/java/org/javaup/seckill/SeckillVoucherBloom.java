package org.javaup.seckill;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.springframework.stereotype.Component;

/** Database catalog existence hint. Never gates admission or persisted order results. */
@Slf4j
@Component
public class SeckillVoucherBloom {
  private final BloomFilterHandlerFactory filters;
  private final SeckillStore store;
  private final MeterRegistry metrics;

  public SeckillVoucherBloom(
      BloomFilterHandlerFactory filters, SeckillStore store, MeterRegistry metrics) {
    this.filters = filters;
    this.store = store;
    this.metrics = metrics;
  }

  public void check(long voucher) {
    if (voucher <= 0) throw new SeckillFailure("VOUCHER_NOT_FOUND", 404);
    try {
      var filter = filters.get(BLOOM_FILTER_HANDLER_VOUCHER);
      String generation = filter.loadedGeneration();
      if (generation == null) {
        fallback("not_ready");
        return;
      }
      if (filter.contains(Long.toString(voucher))) return;
      // Never trust a negative across loss, partial loading or concurrent reinitialization.
      if (!generation.equals(filter.loadedGeneration())) {
        fallback("not_ready");
        return;
      }
    } catch (RuntimeException unavailable) {
      fallback("unavailable");
      return;
    }
    metrics.counter("seckill_v2_bloom_rejected").increment();
    throw new SeckillFailure("VOUCHER_NOT_FOUND", 404);
  }

  /** Additive scan of all database IDs; mark ready only after completing the same generation. */
  public synchronized boolean initialize() {
    try {
      var filter = filters.get(BLOOM_FILTER_HANDLER_VOUCHER);
      String generation = filter.beginLoad();
      long after = 0;
      while (true) {
        var ids = store.catalogVoucherIds(after, 1000);
        if (ids.isEmpty()) break;
        for (long id : ids) filter.ensureInitializedAndAdd(Long.toString(id));
        after = ids.get(ids.size() - 1);
      }
      if (!filter.completeLoad(generation)) throw new IllegalStateException("BLOOM_LOAD_INVALIDATED");
      return true;
    } catch (RuntimeException failure) {
      metrics.counter("seckill_v2_bloom_initialization_failed").increment();
      log.warn("Voucher Bloom initialization incomplete; detail reads fall back to SQL", failure);
      return false;
    }
  }

  /** Before SQL creation. A rolled-back insert leaves only a harmless false positive. */
  public void register(long voucher) {
    try {
      filters.get(BLOOM_FILTER_HANDLER_VOUCHER).ensureInitializedAndAdd(Long.toString(voucher));
    } catch (RuntimeException unavailable) {
      metrics.counter("seckill_v2_bloom_registration_failed").increment();
      throw new SeckillFailure("BLOOM_UNAVAILABLE", 503);
    }
  }

  private void fallback(String reason) {
    metrics.counter("seckill_v2_bloom_fallback", "reason", reason).increment();
  }
}
