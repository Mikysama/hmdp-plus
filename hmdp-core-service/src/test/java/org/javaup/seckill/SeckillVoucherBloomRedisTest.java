package org.javaup.seckill;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.javaup.core.SpringUtil;
import org.javaup.handler.BloomFilterHandler;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.junit.jupiter.api.*;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Real shared Redisson Bloom filter + production Redis admission; no business servers. */
class SeckillVoucherBloomRedisTest {
  static Process process;
  static LettuceConnectionFactory connection;
  static StringRedisTemplate template;
  static RedissonClient redisson;
  static final String FILTER = "bloom-test-voucher";
  RedisAdmissionGateway redis;
  BloomFilterHandler handler;
  SeckillVoucherBloom bloom;

  @BeforeAll static void start() throws Exception {
    int port;
    try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
    var log = Files.createTempFile("seckill-bloom-redis-", ".log");
    process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", "" + port,
        "--save", "", "--appendonly", "no")
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    connection = new LettuceConnectionFactory("127.0.0.1", port);
    connection.afterPropertiesSet();
    template = new StringRedisTemplate(connection);
    template.afterPropertiesSet();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try (var c = connection.getConnection()) { if ("PONG".equals(c.ping())) break; }
      catch (Exception e) { if (System.nanoTime() > deadline) throw e; Thread.sleep(25); }
    }
    var config = new Config();
    config.setThreads(2).setNettyThreads(2);
    config.useSingleServer().setAddress("redis://127.0.0.1:" + port)
        .setConnectionMinimumIdleSize(1).setConnectionPoolSize(4);
    redisson = Redisson.create(config);
  }

  @AfterAll static void stop() throws Exception {
    if (redisson != null) redisson.shutdown();
    if (connection != null) connection.destroy();
    if (process != null) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly(); }
  }

  @BeforeEach void setup() {
    try (var c = connection.getConnection()) { c.serverCommands().flushDb(); }
    try (var prefix = mockStatic(SpringUtil.class)) {
      prefix.when(SpringUtil::getPrefixDistinctionName).thenReturn("bloom-test");
      handler = spy(new BloomFilterHandler(redisson, "voucher", 100000L, 0.01));
    }
    redis = new RedisAdmissionGateway(template, new ObjectMapper(), "admission-test", "stats-test", 1000, 32);
    var factory = mock(BloomFilterHandlerFactory.class);
    when(factory.get(BLOOM_FILTER_HANDLER_VOUCHER)).thenReturn(handler);
    bloom = new SeckillVoucherBloom(factory, redis, new SimpleMeterRegistry());
  }

  void prepare(long id) {
    redis.rebuild(id, 1, 100, 1, "1", 0, System.currentTimeMillis() + 3600000, 0, List.of());
    redis.activate(id, 1);
  }

  @Test void positiveBloomAndWarmCacheStillProduceARealHoldAndStreamRecord() {
    bloom.register(1); prepare(1);
    var cache = new SeckillAdmissionCache(redis, bloom);
    cache.check(1, 7, "r1"); cache.check(1, 8, "r2");
    verify(handler, times(1)).contains("1");
    var held = redis.reserve(1, 7, "r1", redis.issueToken(1, 7), "101");
    assertEquals("HELD", held.state());
    assertEquals(99, redis.inspect(1).stock());
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
  }

  @Test void definiteNegativeDoesNotAllocateTokenOrReadActivityMetadata() {
    bloom.register(1);
    long absent = 999;
    while (handler.contains(Long.toString(absent))) absent++;
    var gateway = spy(redis);
    var factory = mock(BloomFilterHandlerFactory.class);
    when(factory.get(BLOOM_FILTER_HANDLER_VOUCHER)).thenReturn(handler);
    var guard = new SeckillVoucherBloom(factory, gateway, new SimpleMeterRegistry());
    var facade = new SeckillFacade(gateway, mock(SeckillTransactions.class), mock(SeckillStore.class),
        mock(org.javaup.service.IUserInfoService.class), mock(org.javaup.toolkit.SnowflakeIdGenerator.class),
        new SeckillAdmissionCache(gateway, guard), mock(SeckillAdmissionPublisher.class), 10);
    final long id = absent;
    assertEquals("VOUCHER_UNAVAILABLE", assertThrows(SeckillFailure.class,
        () -> facade.issueToken(id, 7)).getCode());
    assertEquals("VOUCHER_UNAVAILABLE", assertThrows(SeckillFailure.class,
        () -> facade.submit(id, 7, "r", "unused", false)).getCode());
    assertFalse(Boolean.TRUE.equals(template.hasKey("admission-test:{"+id+"}:token:7")));
    verify(gateway, never()).activity(id);
  }

  @Test void deletedBitmapDoesNotMisclassifyExistingActivityAndRepairsMembership() {
    bloom.register(1); prepare(1);
    assertTrue(Boolean.TRUE.equals(template.delete(FILTER))); // Keep Redisson configuration.
    assertFalse(handler.contains("1"));
    new SeckillAdmissionCache(redis, bloom).check(1, 7, "r");
    assertTrue(handler.contains("1"));
    assertEquals(100, redis.inspect(1).stock());
  }

  @Test void lostBitmapAfterActivityEndsStillAllowsOriginalRequestRetry() {
    bloom.register(1); prepare(1);
    String token = redis.issueToken(1, 7);
    var original = redis.reserve(1, 7, "original", token, "101");
    template.opsForHash().put("admission-test:{1}:epoch:1:meta", "end", "1");
    template.delete(FILTER);
    var cache = new SeckillAdmissionCache(redis, bloom);
    assertDoesNotThrow(() -> cache.check(1, 7, "original"));
    assertEquals(original, redis.reserve(1, 7, "original", token, "102"));
    assertEquals("ENDED", assertThrows(SeckillFailure.class,
        () -> cache.check(1, 8, "new")).getCode());
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
  }

  @Test void missingConfigurationFallsBackAndRegistrationCanReinitializeIt() {
    bloom.register(1); prepare(1);
    redisson.getBloomFilter(FILTER).delete();
    assertDoesNotThrow(() -> new SeckillAdmissionCache(redis, bloom).check(1, 7, "r"));
    bloom.register(1); // Same path used before recovery activation or creation.
    assertTrue(handler.contains("1"));
  }

  @Test void bloomPositiveCannotInventAnActivity() {
    bloom.register(123);
    assertThrows(RuntimeException.class,
        () -> new SeckillAdmissionCache(redis, bloom).check(123, 7, "r"));
    assertTrue(redis.admissionOutbox(123, null, 100).isEmpty());
  }
}
