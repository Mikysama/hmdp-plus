package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.alibaba.fastjson.JSON;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class SeckillOutboxTest extends SeckillTransactionsTest {
  @SuppressWarnings("unchecked")
  SeckillWorkers workers(RedisAdmissionGateway redis, KafkaTemplate<String, String> kafka) {
    return new SeckillWorkers(
        store,
        tx,
        redis,
        mock(SeckillRecovery.class),
        mock(SeckillFacade.class),
        kafka,
        new SimpleMeterRegistry(),
        mock(SeckillCatalogService.class));
  }

  @Test
  void kafkaFailureRetainsDurableAcceptanceAndPendingEvent() {
    tx.accept(r(7, "r1", 101), 1, false);
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(
            CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
    workers(mock(RedisAdmissionGateway.class), kafka).dispatchKafka();
    assertEquals("PROCESSING", tx.result(1, 7, "r1").status());
    assertEquals(9, SeckillStore.inventory(store.voucher(1, false)).available());
    assertEquals(
        "PENDING",
        j.queryForObject(
            "SELECT status FROM tb_seckill_outbox WHERE event_type='CREATE'", String.class));
  }

  @Test
  void sendBeforeMarkCrashReusesEventId() throws Exception {
    tx.accept(r(7, "r1", 101), 1, false);
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    var worker = workers(mock(RedisAdmissionGateway.class), kafka);
    var row = store.one("SELECT * FROM tb_seckill_outbox WHERE event_type='CREATE'");
    worker.deliver(row);
    worker.deliver(row);
    var payload = org.mockito.ArgumentCaptor.forClass(String.class);
    verify(kafka, times(2)).send(eq("seckill-order-v2"), eq("1"), payload.capture());
    assertEquals(payload.getAllValues().get(0), payload.getAllValues().get(1));
    assertEquals("create:101", JSON.parseObject(payload.getValue()).getString("eventId"));
  }

  @Test
  void activeLeasePreventsAnotherDispatcherClaim() {
    tx.accept(r(7, "r1", 101), 1, false);
    j.update(
        "UPDATE tb_seckill_outbox SET status='SENDING',lease_until=? WHERE event_type='CREATE'",
        LocalDateTime.now().plusMinutes(1));
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    workers(mock(RedisAdmissionGateway.class), kafka).dispatchKafka();
    verifyNoInteractions(kafka);
  }

  @Test
  void projectionGapIsRetriedWithoutMarkingSent() {
    tx.accept(r(7, "r1", 101), 1, false);
    var redis = mock(RedisAdmissionGateway.class);
    when(redis.project(
            anyLong(),
            anyLong(),
            anyLong(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong()))
        .thenThrow(new IllegalStateException("SEQUENCE_GAP"));
    workers(redis, mock(KafkaTemplate.class)).dispatch();
    assertEquals(
        "PENDING",
        j.queryForObject(
            "SELECT status FROM tb_seckill_outbox WHERE event_type='REDIS'", String.class));
    assertEquals(
        1,
        j.queryForObject(
            "SELECT attempts FROM tb_seckill_outbox WHERE event_type='REDIS'", Integer.class));
  }
}
