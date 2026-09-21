package org.javaup.cache;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Centralized cache TTL policy. A small positive jitter prevents cache entries
 * written in the same batch from expiring at exactly the same time.
 */
@Component
public class CacheTtlPolicy {

    private final int jitterPercent;
    private final long seckillVoucherBaseTtlSeconds;

    public CacheTtlPolicy(
            @Value("${cache.ttl.jitter-percent:20}") int jitterPercent,
            @Value("${cache.ttl.seckill-voucher-base-seconds:600}") long seckillVoucherBaseTtlSeconds) {
        this.jitterPercent = Math.max(jitterPercent, 0);
        this.seckillVoucherBaseTtlSeconds = Math.max(seckillVoucherBaseTtlSeconds, 1L);
    }

    public long withJitter(long baseTtl) {
        long safeBaseTtl = Math.max(baseTtl, 1L);
        long jitterProduct = safeMultiply(safeBaseTtl, jitterPercent);
        long maxJitter = jitterProduct == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : jitterProduct / 100L + (jitterProduct % 100L == 0 ? 0L : 1L);
        if (maxJitter <= 0) {
            return safeBaseTtl;
        }
        long jitter = maxJitter == Long.MAX_VALUE
                ? ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)
                : ThreadLocalRandom.current().nextLong(maxJitter + 1L);
        return safeAdd(safeBaseTtl, jitter);
    }

    /**
     * Returns a jittered short TTL that never exceeds the business deadline.
     */
    public long forSeckillVoucher(long secondsUntilEnd) {
        long safeDeadline = Math.max(secondsUntilEnd, 1L);
        return Math.min(safeDeadline, withJitter(seckillVoucherBaseTtlSeconds));
    }

    private long safeMultiply(long value, int multiplier) {
        try {
            return Math.multiplyExact(value, (long) multiplier);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private long safeAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }
}
