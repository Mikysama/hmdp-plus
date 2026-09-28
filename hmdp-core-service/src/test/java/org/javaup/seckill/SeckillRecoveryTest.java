package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.Test;

class SeckillRecoveryTest extends SeckillTransactionsTest {
  @Test
  void redisOutageDoesNotFreezeAcceptedDatabaseWork() {
    tx.accept(r(7, "r1", 101), 1, false);
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.available()).thenReturn(false);
    var recovery = new SeckillRecovery(store, tx, redis);
    assertThrows(SeckillFailure.class, () -> recovery.recover(1));
    assertEquals("OPEN", SeckillStore.str(store.voucher(1, false), "admission_state"));
    assertEquals("SUCCEEDED", tx.complete(1, 101).status());
  }

  @Test
  void invariantPauseIsCommittedDespiteReportedFailure() {
    tx.accept(r(7, "r1", 101), 1, false);
    j.update("DELETE FROM tb_seckill_active_purchase");
    var redis = mock(RedisAdmissionGateway.class);
    var recovery = new SeckillRecovery(store, tx, redis);
    assertThrows(SeckillFailure.class, () -> recovery.audit(1));
    assertEquals("PAUSED", SeckillStore.str(store.voucher(1, false), "admission_state"));
    assertEquals(
        "INVARIANT", j.queryForObject("SELECT phase FROM tb_seckill_recovery", String.class));
  }

  @Test
  void redisProjectionFailureLeavesRecoverableFreeze() {
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.available()).thenReturn(true);
    doThrow(new IllegalStateException("redis down during snapshot"))
        .when(redis)
        .rebuild(
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyList());
    var recovery = new SeckillRecovery(store, tx, redis);
    assertThrows(IllegalStateException.class, () -> recovery.recover(1));
    assertEquals("REBUILDING", SeckillStore.str(store.voucher(1, false), "admission_state"));
    assertEquals("RETRY", j.queryForObject("SELECT status FROM tb_seckill_recovery", String.class));
    assertEquals(10, SeckillStore.inventory(store.voucher(1, false)).available());
  }

  @Test
  void recoveryCannotOpenDatabaseRelationshipCorruption() {
    tx.accept(r(7, "r1", 101), 1, false);
    j.update("DELETE FROM tb_seckill_active_purchase");
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.available()).thenReturn(true);
    assertThrows(SeckillFailure.class, () -> new SeckillRecovery(store, tx, redis).recover(1));
    assertEquals("PAUSED", SeckillStore.str(store.voucher(1, false), "admission_state"));
    assertEquals(
        "INVARIANT", j.queryForObject("SELECT phase FROM tb_seckill_recovery", String.class));
    verify(redis, never())
        .rebuild(
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyList());
  }
}
