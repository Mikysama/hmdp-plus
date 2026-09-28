package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Real Redis intent storage; only the Kafka transport is replaced to inject ACK failures. */
class SeckillStreamRelayTest {
  static Process process;
  static LettuceConnectionFactory connection;
  static StringRedisTemplate template;
  RedisAdmissionGateway redis;
  SeckillStore store;
  SeckillAdmissionPublisher publisher;

  @BeforeAll static void startRedis() throws Exception {
    int port;
    try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
    var log = Files.createTempFile("seckill-stream-redis-", ".log");
    process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", "" + port,
        "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
    connection = new LettuceConnectionFactory("127.0.0.1", port);
    connection.afterPropertiesSet();
    template = new StringRedisTemplate(connection);
    template.afterPropertiesSet();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try (var c = connection.getConnection()) { if ("PONG".equals(c.ping())) break; }
      catch (Exception e) { if (System.nanoTime() > deadline) throw e; Thread.sleep(25); }
    }
  }

  @AfterAll static void stopRedis() throws Exception {
    if (connection != null) connection.destroy();
    if (process != null) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly(); }
  }

  @BeforeEach void setup() {
    try (var c = connection.getConnection()) { c.serverCommands().flushDb(); }
    redis = new RedisAdmissionGateway(template, new ObjectMapper(), "stream-test", "stats-test", 10000, 10000);
    redis.rebuild(1, 1, 1000, 1, "1", 0, System.currentTimeMillis() + 3600000, 0, List.of());
    redis.activate(1, 1);
    store = mock(SeckillStore.class);
    when(store.vouchers(0, 100)).thenReturn(List.of(1L));
    publisher = mock(SeckillAdmissionPublisher.class);
  }

  SeckillAdmissionRelay relay() {
    return new SeckillAdmissionRelay(store, redis, publisher, new SimpleMeterRegistry());
  }

  @Test void failedSendSurvivesWorkerRestartAndNeedsNoHttpRetry() {
    var held = redis.reserve(1, 7, "r1", redis.issueToken(1, 7), "101", true);
    doThrow(new SeckillFailure("DELIVERY_UNCONFIRMED", 503)).doNothing()
        .when(publisher).publish(held, true);
    relay().relay();
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
    relay().relay(); // A fresh worker discovers the durable intent.
    assertTrue(redis.admissionOutbox(1, null, 100).isEmpty());
    assertEquals(held, redis.findReservation(1, 7, "r1"));
  }

  @Test void httpRetryCannotChangeTheStreamDeliveryMode() {
    var token = redis.issueToken(1, 7);
    var original = redis.reserve(1, 7, "auto_7_1", token, "101", true);
    assertEquals(original, redis.reserve(1, 7, "auto_7_1", token, "102", true));
    var failure = assertThrows(IllegalStateException.class,
        () -> redis.reserve(1, 7, "auto_7_1", token, "103", false));
    assertTrue(failure.getMessage().contains("AUTO_ISSUE_MISMATCH"));
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
    relay().relay();
    assertTrue(redis.admissionOutbox(1, null, 100).isEmpty());
    verify(publisher).publish(original, true);
  }

  @Test void ackBeforeDeleteFailureReplaysTheSameIdentity() {
    var held = redis.reserve(1, 7, "r1", redis.issueToken(1, 7), "101");
    var failingRedis = spy(redis);
    doThrow(new IllegalStateException("lost redis connection after broker ack"))
        .doCallRealMethod().when(failingRedis).acknowledgeAdmission(eq(1L), anyString());
    new SeckillAdmissionRelay(store, failingRedis, publisher, new SimpleMeterRegistry()).relay();
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
    relay().relay();
    assertTrue(redis.admissionOutbox(1, null, 100).isEmpty());
    verify(publisher, times(2)).publish(held, false);
  }

  @Test void malformedRecordIsRetainedWithoutBlockingFollowingMessages() {
    template.opsForStream().add("stream-test:{1}:admission-outbox", java.util.Map.of("reservation", "broken"));
    redis.reserve(1, 7, "r1", redis.issueToken(1, 7), "101");
    relay().relay();
    var remaining = redis.admissionOutbox(1, null, 100);
    assertEquals(1, remaining.size());
    assertEquals("broken", remaining.get(0).getValue().get("reservation"));
  }

  @Test void continuousNewEntriesCannotStarveAnEarlierFailedDelivery() {
    var first = redis.reserve(1, 7, "first", redis.issueToken(1, 7), "101");
    for (int i = 0; i < 99; i++) redis.reserve(1, 1000+i, "r"+i, redis.issueToken(1, 1000+i), ""+(1000+i));
    doThrow(new SeckillFailure("DELIVERY_UNCONFIRMED", 503)).doNothing()
        .when(publisher).publish(first, false);
    var worker = relay();
    worker.relay();
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
    redis.renew(1, 1);
    for (int i = 0; i < 100; i++) redis.reserve(1, 2000+i, "n"+i, redis.issueToken(1, 2000+i), ""+(2000+i));
    worker.relay(); // Wrap the voucher enumeration.
    worker.relay();
    assertTrue(redis.admissionOutbox(1, null, 100).stream()
        .noneMatch(row -> redis.decodeAdmission(1, row.getValue()).orderId().equals("101")));
  }

  @Test void rebuildDoesNotDiscardOldEpochIntents() {
    var held = redis.reserve(1, 7, "r1", redis.issueToken(1, 7), "101");
    redis.rebuild(1, 2, 1000, 1, "1", 0, System.currentTimeMillis() + 3600000, 0, List.of());
    redis.activate(1, 2);
    relay().relay();
    assertTrue(redis.admissionOutbox(1, null, 100).isEmpty());
    verify(publisher).publish(held, false); // Consumer's epoch check decides the business result.
    assertEquals(1000, redis.inspect(1).stock());
  }
}
