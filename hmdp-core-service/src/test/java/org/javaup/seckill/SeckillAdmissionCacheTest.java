package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.Test;

class SeckillAdmissionCacheTest {
  @Test
  void repeatedActivityLookupsUseLocalCacheAndNeverNeedDatabase() {
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.activity(1)).thenReturn(new RedisAdmissionGateway.Activity("1", 0, Long.MAX_VALUE));
    var cache = new SeckillAdmissionCache(redis);
    cache.check(1, 7, "first");
    cache.check(1, 8, "second");
    verify(redis, times(1)).activity(1);
    verify(redis, never()).findReservation(anyLong(), anyLong(), anyString());
  }

  @Test
  void closedActivityRejectsNewIntentButDoesNotBlockExistingRequestRetry() {
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.activity(1)).thenReturn(new RedisAdmissionGateway.Activity("1", 0, 1));
    var cache = new SeckillAdmissionCache(redis);
    assertEquals(
        "ENDED", assertThrows(SeckillFailure.class, () -> cache.check(1, 7, "new")).getCode());
    when(redis.findReservation(1, 7, "old"))
        .thenReturn(
            new RedisAdmissionGateway.Reservation("old", "101", 1, 7, 1, 0, 100, "HELD"));
    assertDoesNotThrow(() -> cache.check(1, 7, "old"));
  }
}
