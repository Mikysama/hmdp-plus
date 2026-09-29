package org.javaup.seckill;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.javaup.handler.BloomFilterHandler;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SeckillVoucherBloomTest {
  final BloomFilterHandler filter = mock(BloomFilterHandler.class);
  final BloomFilterHandlerFactory factory = mock(BloomFilterHandlerFactory.class);
  final RedisAdmissionGateway redis = mock(RedisAdmissionGateway.class);
  SeckillVoucherBloom bloom;

  @BeforeEach void setup() {
    when(factory.get(BLOOM_FILTER_HANDLER_VOUCHER)).thenReturn(filter);
    bloom = new SeckillVoucherBloom(factory, redis, new SimpleMeterRegistry());
  }

  @Test void negativeAndNoAdmissionRejectsBeforeActivityLoading() {
    var cache = new SeckillAdmissionCache(redis, bloom);
    var failure = assertThrows(SeckillFailure.class, () -> cache.check(999, 7, "request"));
    assertEquals("VOUCHER_UNAVAILABLE", failure.getCode());
    verify(redis, never()).activity(anyLong());
    verify(redis, never()).findReservation(anyLong(), anyLong(), anyString());
  }

  @Test void bloomPositiveStillRequiresRealActivityAndLuaChecks() {
    when(filter.contains("1")).thenReturn(true);
    when(redis.activity(1)).thenThrow(new IllegalStateException("ADMISSION_UNINITIALIZED"));
    assertThrows(IllegalStateException.class,
        () -> new SeckillAdmissionCache(redis, bloom).check(1, 7, "request"));
    verify(redis).activity(1);
    verify(redis, never()).hasAdmission(anyLong());
  }

  @Test void missingBloomEntryDoesNotRejectAnExistingActivityAndIsRepaired() {
    when(redis.hasAdmission(1)).thenReturn(true);
    when(redis.activity(1)).thenReturn(new RedisAdmissionGateway.Activity("1", 0, Long.MAX_VALUE));
    assertDoesNotThrow(() -> new SeckillAdmissionCache(redis, bloom).check(1, 7, "request"));
    verify(filter).ensureInitializedAndAdd("1");
  }

  @Test void filterFailureFallsBackToOriginalRedisPath() {
    when(filter.contains("1")).thenThrow(new IllegalStateException("missing bloom config"));
    when(redis.activity(1)).thenReturn(new RedisAdmissionGateway.Activity("1", 0, Long.MAX_VALUE));
    assertDoesNotThrow(() -> new SeckillAdmissionCache(redis, bloom).check(1, 7, "request"));
  }

  @Test void repairFailureDoesNotRejectExistingActivity() {
    when(redis.hasAdmission(1)).thenReturn(true);
    doThrow(new IllegalStateException("bloom unavailable")).when(filter).ensureInitializedAndAdd("1");
    assertDoesNotThrow(() -> bloom.check(1));
  }

  @Test void registrationFailureIsExplicitSoCreationCannotCommitAnUnindexedVoucher() {
    doThrow(new IllegalStateException("down")).when(filter).ensureInitializedAndAdd("1");
    assertEquals("BLOOM_UNAVAILABLE",
        assertThrows(SeckillFailure.class, () -> bloom.register(1)).getCode());
  }
}
