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

/** Real Redis/Redisson and SQL catalog; no production servers. */
class SeckillVoucherBloomRedisTest {
  static Process process;
  static LettuceConnectionFactory connection;
  static StringRedisTemplate template;
  static RedissonClient redisson;
  static final String FILTER = "bloom-test-voucher";
  RedisAdmissionGateway redis;
  BloomFilterHandler handler;
  SeckillVoucherBloom bloom;
  SeckillStore store;
  org.springframework.jdbc.core.JdbcTemplate jdbc;
  SeckillCatalogService catalog;

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
    jdbc = new org.springframework.jdbc.core.JdbcTemplate(
        new org.springframework.jdbc.datasource.DriverManagerDataSource(
            "jdbc:h2:mem:" + java.util.UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", ""));
    jdbc.execute("CREATE TABLE tb_voucher(id BIGINT PRIMARY KEY,title VARCHAR(64),type INT)");
    jdbc.execute("CREATE TABLE tb_seckill_voucher(voucher_id BIGINT PRIMARY KEY,stock INT,admission_state VARCHAR(16))");
    jdbc.update("INSERT INTO tb_voucher VALUES(1,'Database only voucher',1),(2,'Ordinary voucher',0)");
    jdbc.update("INSERT INTO tb_seckill_voucher VALUES(1,100,'PAUSED')");
    store = spy(new SeckillStore(jdbc));
    bloom = new SeckillVoucherBloom(factory, store, new SimpleMeterRegistry());
    catalog = new SeckillCatalogService(store, mock(SeckillTransactions.class),
        mock(org.javaup.toolkit.SnowflakeIdGenerator.class), mock(org.javaup.context.DelayQueueContext.class), 120, bloom);
  }

  void prepare(long id) {
    redis.rebuild(id, 1, 100, 1, "1", 0, System.currentTimeMillis() + 3600000, 0, List.of());
    redis.activate(id, 1);
  }

  @Test void uncachedPausedVoucherStillLoadsFromDatabase() {
    assertTrue(bloom.initialize());
    assertFalse(template.hasKey("admission-test:{1}:active"));
    assertEquals("Database only voucher", catalog.get(1).get("title"));
    assertEquals("PAUSED", catalog.get(1).get("admissionState"));
    assertTrue(handler.contains("2")); // Initialization covers ordinary IDs as well.
  }

  @Test void definiteNegativeAvoidsBothDatabaseQueries() {
    assertTrue(bloom.initialize());
    long absent = 999;
    while (handler.contains(Long.toString(absent))) absent++;
    clearInvocations(store);
    final long id = absent;
    assertEquals("VOUCHER_NOT_FOUND", assertThrows(SeckillFailure.class, () -> catalog.get(id)).getCode());
    verifyNoInteractions(store);
  }

  @Test void falsePositiveStillChecksDatabase() {
    assertTrue(bloom.initialize());
    bloom.register(999); // Simulates a registered ID whose SQL insert rolled back.
    clearInvocations(store);
    assertEquals("VOUCHER_NOT_FOUND", assertThrows(SeckillFailure.class, () -> catalog.get(999)).getCode());
    verify(store).voucher(999, false);
  }

  @Test void partialIndexCannotRejectExistingDatabaseVoucher() {
    bloom.register(999);
    assertNull(handler.loadedGeneration());
    assertEquals("Database only voucher", catalog.get(1).get("title"));
  }

  @Test void missingBitmapFallsBackAndPartialRepairDoesNotMarkReady() {
    assertTrue(bloom.initialize());
    template.delete(FILTER);
    bloom.register(999); // Must invalidate the old READY even if read hasn't observed the loss.
    assertNull(handler.loadedGeneration());
    assertEquals("Database only voucher", catalog.get(1).get("title"));
    assertTrue(bloom.initialize());
    assertNotNull(handler.loadedGeneration());
    assertTrue(handler.contains("1"));
  }

  @Test void missingConfigFallsBackAndReinitializationRemainsIncomplete() {
    assertTrue(bloom.initialize());
    template.delete(org.redisson.RedissonObject.suffixName(FILTER, "config"));
    assertEquals("Database only voucher", catalog.get(1).get("title"));
    bloom.register(999);
    assertNull(handler.loadedGeneration());
    assertEquals("Database only voucher", catalog.get(1).get("title"));
  }

  @Test void dataLossDuringFullLoadCannotPublishPartialIndex() {
    doAnswer(call -> {
      var ids = (List<Long>) call.callRealMethod();
      if ((long) call.getArgument(0) == 0) {
        template.delete(FILTER);
        bloom.register(999);
      }
      return ids;
    }).when(store).catalogVoucherIds(anyLong(), anyInt());
    assertFalse(bloom.initialize());
    assertNull(handler.loadedGeneration());
    assertEquals("Database only voucher", catalog.get(1).get("title"));
  }

  @Test void failedScanDoesNotLeaveReadyStateFromPreviousLoad() {
    assertTrue(bloom.initialize());
    doThrow(new IllegalStateException("SQL unavailable")).when(store).catalogVoucherIds(anyLong(), anyInt());
    assertFalse(bloom.initialize());
    assertNull(handler.loadedGeneration());
    assertEquals("Database only voucher", catalog.get(1).get("title"));
  }

  @Test void concurrentLoadersCannotPublishAnObsoleteGeneration() {
    String first = handler.beginLoad();
    String second = handler.beginLoad();
    assertFalse(handler.completeLoad(first));
    assertNull(handler.loadedGeneration());
    assertTrue(handler.completeLoad(second));
    assertNotNull(handler.loadedGeneration());
  }

  @Test void newVoucherRegisteredBeforeInsertCanBeReadAfterReady() {
    assertTrue(bloom.initialize());
    bloom.register(3);
    jdbc.update("INSERT INTO tb_voucher VALUES(3,'New voucher',1)");
    jdbc.update("INSERT INTO tb_seckill_voucher VALUES(3,10,'PAUSED')");
    assertEquals("New voucher", catalog.get(3).get("title"));
  }

  @Test void admissionAndStreamDoNotDependOnCatalogBloom() {
    prepare(1);
    doThrow(new IllegalStateException("Bloom unavailable")).when(handler).contains(anyString());
    var cache = new SeckillAdmissionCache(redis);
    var facade = new SeckillFacade(redis, mock(SeckillTransactions.class), mock(SeckillStore.class),
        mock(org.javaup.service.IUserInfoService.class), mock(org.javaup.toolkit.SnowflakeIdGenerator.class),
        cache, mock(SeckillAdmissionPublisher.class), 10);
    String token = facade.issueToken(1, 7);
    cache.check(1, 7, "r1");
    var held = redis.reserve(1, 7, "r1", token, "101");
    assertEquals("HELD", held.state());
    assertEquals(99, redis.inspect(1).stock());
    assertEquals(1, redis.admissionOutbox(1, null, 100).size());
    verify(handler, never()).contains(anyString());
  }
}
