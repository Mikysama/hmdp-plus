package org.javaup.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheTtlPolicyTest {

    @Test
    void shouldAddBoundedPositiveJitter() {
        CacheTtlPolicy policy = new CacheTtlPolicy(20, 600);

        for (int i = 0; i < 100; i++) {
            long ttl = policy.withJitter(30);
            assertTrue(ttl >= 30 && ttl <= 36);
        }
    }

    @Test
    void shouldNeverExceedSeckillDeadline() {
        CacheTtlPolicy policy = new CacheTtlPolicy(20, 600);

        assertEquals(120, policy.forSeckillVoucher(120));
        long ttl = policy.forSeckillVoucher(3600);
        assertTrue(ttl >= 600 && ttl <= 720);
    }

    @Test
    void shouldClampNonPositiveTtl() {
        CacheTtlPolicy policy = new CacheTtlPolicy(0, 600);

        assertEquals(1, policy.withJitter(0));
        assertEquals(1, policy.forSeckillVoucher(-1));
    }

    @Test
    void shouldAlsoJitterShortNullCacheTtl() {
        CacheTtlPolicy policy = new CacheTtlPolicy(20, 600);

        for (int i = 0; i < 100; i++) {
            long ttl = policy.withJitter(2);
            assertTrue(ttl >= 2 && ttl <= 3);
        }
    }
}
