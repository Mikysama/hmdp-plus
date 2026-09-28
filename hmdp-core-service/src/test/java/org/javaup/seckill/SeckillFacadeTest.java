package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.service.IUserInfoService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.junit.jupiter.api.Test;

class SeckillFacadeTest {
  final RedisAdmissionGateway redis = mock(RedisAdmissionGateway.class);
  final SeckillTransactions tx = mock(SeckillTransactions.class);
  final SeckillStore store = mock(SeckillStore.class);
  final IUserInfoService users = mock(IUserInfoService.class);
  final SeckillAdmissionCache cache = mock(SeckillAdmissionCache.class);
  final SeckillAdmissionPublisher publisher = mock(SeckillAdmissionPublisher.class);
  final RedisAdmissionGateway.Reservation reservation =
      new RedisAdmissionGateway.Reservation("request", "101", 1, 7, 1, 2, 100, "HELD");

  SeckillFacade facade() {
    when(redis.tryEnter(anyLong(), anyString())).thenReturn(true);
    when(redis.reserve(anyLong(), anyLong(), anyString(), anyString(), anyString(), anyBoolean()))
        .thenReturn(reservation);
    return new SeckillFacade(
        redis, tx, store, users, mock(SnowflakeIdGenerator.class), cache, publisher, 1);
  }

  @Test
  void qualifiedRequestMustQueueWithoutAnyDatabaseOrMemberAccess() {
    var f = facade();
    assertEquals("QUEUED", f.submit(1, 7, "request", "token", false).status());
    verifyNoInteractions(store, tx, users);
    var order = inOrder(cache, redis, publisher);
    order.verify(cache).check(1, 7, "request");
    order.verify(redis).tryEnter(eq(1L), anyString());
    order.verify(redis).reserve(eq(1L), eq(7L), eq("request"), eq("token"), anyString(), eq(false));
    order.verify(publisher).publish(reservation, false);
  }

  @Test
  void soldOutTerminatesWithoutDatabaseOrMemberAccess() {
    var f = facade();
    when(redis.reserve(anyLong(), anyLong(), anyString(), anyString(), anyString(), anyBoolean()))
        .thenThrow(new IllegalStateException("SOLD_OUT"));
    assertEquals(
        "SOLD_OUT",
        assertThrows(SeckillFailure.class, () -> f.submit(1, 7, "request", "token", false))
            .getCode());
    verifyNoInteractions(store, tx, users, publisher);
  }

  @Test
  void unknownSendKeepsReservationAndRetryReusesMessageIdentity() {
    var f = facade();
    doThrow(new SeckillFailure("DELIVERY_UNCONFIRMED", 503))
        .doNothing()
        .when(publisher)
        .publish(reservation, false);
    assertEquals(
        "DELIVERY_UNCONFIRMED",
        assertThrows(SeckillFailure.class, () -> f.submit(1, 7, "request", "token", false))
            .getCode());
    assertEquals("101", f.submit(1, 7, "request", "token", false).orderId());
    verifyNoInteractions(store, tx, users);
    verify(publisher, times(2)).publish(reservation, false);
    verify(redis, never())
        .project(
            anyLong(),
            anyLong(),
            anyLong(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong());
  }

  @Test
  void pendingQueryCannotInferSuccessFromRedis() {
    var f = facade();
    when(tx.result(1, 7, "request"))
        .thenReturn(new SeckillResult("request", null, "NOT_FOUND", null, null));
    when(redis.findReservation(1, 7, "request")).thenReturn(reservation);
    assertEquals("PENDING", f.result(1, 7, "request").status());
    when(tx.result(1, 7, "request"))
        .thenReturn(new SeckillResult("request", "101", "SUCCEEDED", null, null));
    assertEquals("SUCCEEDED", f.result(1, 7, "request").status());
  }
}
