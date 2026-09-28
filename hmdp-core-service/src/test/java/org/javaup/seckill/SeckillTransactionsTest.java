package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

class SeckillTransactionsTest {
  JdbcTemplate j;
  SeckillTransactions tx;
  SeckillStore store;

  @BeforeEach
  void setup() throws Exception {
    var ds =
        new DriverManagerDataSource(
            "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
    j = new JdbcTemplate(ds);
    j.execute(
        "CREATE TABLE tb_seckill_voucher(id BIGINT,voucher_id BIGINT PRIMARY KEY,init_stock"
            + " INT,stock INT,reserved_stock INT,sold_stock INT,version BIGINT,rule_version"
            + " BIGINT,admission_epoch BIGINT,admission_state VARCHAR(16),projection_seq"
            + " BIGINT,begin_time TIMESTAMP,end_time TIMESTAMP,allowed_levels VARCHAR(64),min_level"
            + " INT,update_time TIMESTAMP)");
    j.execute("CREATE TABLE tb_voucher(id BIGINT PRIMARY KEY,status INT,shop_id BIGINT)");
    j.execute(
        "CREATE TABLE tb_voucher_order(id BIGINT PRIMARY KEY,user_id BIGINT,voucher_id"
            + " BIGINT,status INT,create_time TIMESTAMP,update_time TIMESTAMP)");
    String ddl =
        Files.readString(Path.of("../sql/v2/new_tables.sql"))
            .replace("___N__", "")
            .replaceAll("(?i) COLLATE utf8mb4_bin", "")
            .replaceAll("(?i) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "");
    int index = 0;
    for (String sql : ddl.split(";"))
      if (!sql.isBlank())
        j.execute(sql.replaceAll("(?i)(KEY) `([a-z_]+)`", "$1 `$2_" + (index++) + "`"));
    j.update(
        "INSERT INTO tb_seckill_voucher"
            + " VALUES(1,1,10,10,0,0,0,0,1,'OPEN',0,?,?,NULL,NULL,CURRENT_TIMESTAMP)",
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1));
    j.update("INSERT INTO tb_voucher VALUES(1,1,9)");
    store = new SeckillStore(j);
    tx =
        new SeckillTransactions(
            store, new TransactionTemplate(new DataSourceTransactionManager(ds)), 60);
    org.springframework.test.util.ReflectionTestUtils.setField(tx, "activityZone", "UTC");
  }

  Reservation r(long user, String request, long order) {
    return new Reservation(request, "" + order, 1, user, 1, 0, System.currentTimeMillis(), "HELD");
  }

  @Test
  void queuedConsumptionCreatesOrderAtomicallyWithoutCreateOutbox() {
    var held = r(7, "queued", 991);
    assertEquals("SUCCEEDED", tx.fulfill(held, 1, false, 60).status());
    assertEquals("SUCCEEDED", tx.fulfill(held, 1, false, 60).status());
    assertEquals(new Inventory(10, 9, 0, 1), store.inventory(store.voucher(1, false)));
    assertEquals(1, j.queryForObject("SELECT COUNT(*) FROM tb_voucher_order", Integer.class));
    assertEquals(
        0,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='CREATE'", Integer.class));
  }

  @Test
  void failedQueuedOrderInsertRollsBackAcceptanceAndOutboxTogether() {
    j.update("INSERT INTO tb_voucher_order VALUES(991,88,1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)");
    assertThrows(RuntimeException.class, () -> tx.fulfill(r(7, "queued", 991), 1, false, 60));
    assertEquals("NOT_FOUND", tx.result(1, 7, "queued").status());
    assertEquals(new Inventory(10, 10, 0, 0), store.inventory(store.voucher(1, false)));
    assertEquals(0, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_outbox", Integer.class));
    assertEquals(
        0, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_active_purchase", Integer.class));
  }

  @Test
  void expiredQueuedMessageAndStaleEpochCannotConsumeCurrentStock() {
    var expired =
        new Reservation("expired", "991", 1, 7, 1, 0, System.currentTimeMillis() - 120000, "HELD");
    assertEquals("QUEUE_EXPIRED", tx.fulfill(expired, 1, false, 60).reasonCode());
    j.update("UPDATE tb_seckill_voucher SET admission_epoch=2");
    assertEquals("STALE_EPOCH", tx.fulfill(r(8, "stale", 992), 1, false, 60).reasonCode());
    assertEquals(new Inventory(10, 10, 0, 0), store.inventory(store.voucher(1, false)));
    assertEquals(
        1,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REDIS'", Integer.class));
  }

  @Test
  void automaticQueuedFailureSchedulesAnotherCandidateWithoutReusingFailedRound() {
    tx.subscribe(1, 7, true);
    var held =
        new Reservation("auto_7_0", "991", 1, 7, 1, 0, System.currentTimeMillis() - 120000, "HELD");
    assertEquals("FAILED", tx.fulfill(held, 1, true, 60).status());
    assertEquals(
        "FAILED",
        j.queryForObject(
            "SELECT status FROM tb_seckill_subscription WHERE user_id=7", String.class));
    assertEquals(
        1,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REFILL'", Integer.class));
    tx.fulfill(held, 1, true, 60);
    assertEquals(
        1,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REFILL'", Integer.class));
  }

  @Test
  void concurrentQueuedReplaysAndOrphanTerminationConvergeToOneOutcome() throws Exception {
    var held = r(7, "race-queued", 991);
    var start = new CountDownLatch(1);
    try (var pool = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<?>>();
      for (int i = 0; i < 20; i++) {
        final boolean orphan = i % 2 == 0;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  if (orphan) tx.resolveOrphan(held);
                  else tx.fulfill(held, 1, false, 60);
                  return null;
                }));
      }
      start.countDown();
      for (var future : futures) future.get();
    }
    String status = tx.result(1, 7, "race-queued").status();
    assertTrue(Set.of("SUCCEEDED", "FAILED").contains(status));
    int sold = "SUCCEEDED".equals(status) ? 1 : 0;
    assertEquals(new Inventory(10, 10 - sold, 0, sold), store.inventory(store.voucher(1, false)));
    assertEquals(1, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_request", Integer.class));
    assertEquals(sold, j.queryForObject("SELECT COUNT(*) FROM tb_voucher_order", Integer.class));
  }

  @Test
  void sameRequestDoesNotReserveTwice() {
    var r = r(7, "r1", 101);
    tx.accept(r, 1, false);
    tx.accept(r, 1, false);
    assertEquals(9, store.inventory(store.voucher(1, false)).available());
    assertEquals(1, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_request", Integer.class));
  }

  @Test
  void duplicateUserFailsWithoutTakingAnotherUnit() {
    tx.accept(r(7, "r1", 101), 1, false);
    assertEquals("FAILED", tx.accept(r(7, "r2", 102), 1, false).status());
    assertEquals(9, store.inventory(store.voucher(1, false)).available());
  }

  @Test
  void committedOrderAndRepeatedCancelRestoreOnce() {
    tx.accept(r(7, "r1", 101), 1, false);
    tx.complete(1, 101);
    tx.complete(1, 101);
    tx.cancel(1, 7, 101);
    tx.cancel(1, 7, 101);
    assertEquals(new Inventory(10, 10, 0, 0), store.inventory(store.voucher(1, false)));
  }

  @Test
  void oldCancellationCannotTouchNewPurchase() {
    tx.accept(r(7, "r1", 101), 1, false);
    tx.complete(1, 101);
    tx.cancel(1, 7, 101);
    tx.accept(r(7, "r2", 102), 1, false);
    tx.cancel(1, 7, 101);
    assertEquals(
        102L, j.queryForObject("SELECT order_id FROM tb_seckill_active_purchase", Long.class));
  }

  @Test
  void expiryWinsAgainstLateMessage() {
    tx.accept(r(7, "r1", 101), 1, false);
    j.update("UPDATE tb_seckill_request SET expires_at=?", LocalDateTime.now().minusSeconds(1));
    tx.complete(1, 101);
    assertEquals("FAILED", tx.result(1, 7, "r1").status());
    assertEquals(10, store.inventory(store.voucher(1, false)).available());
  }

  @Test
  void terminalOrphanCannotBeAcceptedLater() {
    var r = r(7, "r1", 101);
    tx.resolveOrphan(r);
    assertEquals("FAILED", tx.accept(r, 1, false).status());
    assertEquals(10, store.inventory(store.voucher(1, false)).available());
  }

  @Test
  void cancelledOrderCannotBeCancelledByAnotherUser() {
    tx.accept(r(7, "r1", 101), 1, false);
    tx.complete(1, 101);
    assertThrows(SeckillFailure.class, () -> tx.cancel(1, 8, 101));
  }

  @Test
  void concurrentCancelRestoresOneUnit() throws Exception {
    tx.accept(r(7, "r1", 101), 1, false);
    tx.complete(1, 101);
    try (var pool = Executors.newFixedThreadPool(12)) {
      var tasks = new ArrayList<Callable<Object>>();
      for (int n = 0; n < 100; n++) tasks.add(() -> tx.cancel(1, 7, 101));
      for (var f : pool.invokeAll(tasks)) f.get();
    }
    assertEquals(10, store.inventory(store.voucher(1, false)).available());
  }

  @Test
  void adjustmentIsIdempotentAndRejectsChangedPayload() {
    tx.adjust(1, "a", 0, 12);
    tx.adjust(1, "a", 0, 12);
    assertEquals(12, store.inventory(store.voucher(1, false)).available());
    assertThrows(SeckillFailure.class, () -> tx.adjust(1, "a", 0, 13));
  }

  @Test
  void rollbackCannotExposeProcessingOrLeaveInventoryReserved() {
    assertThrows(
        IllegalStateException.class,
        () ->
            tx.in(
                () -> {
                  tx.accept(r(7, "r1", 101), 1, false);
                  throw new IllegalStateException("connection failed before commit");
                }));
    assertEquals("NOT_FOUND", tx.result(1, 7, "r1").status());
    assertEquals(10, SeckillStore.inventory(store.voucher(1, false)).available());
    assertEquals(0, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_outbox", Integer.class));
  }

  @Test
  void concurrentSameRequestHasOneReservation() throws Exception {
    try (var pool = Executors.newFixedThreadPool(12)) {
      var tasks = new ArrayList<Callable<Object>>();
      for (int n = 0; n < 50; n++) tasks.add(() -> tx.accept(r(7, "same", 101), 1, false));
      for (var f : pool.invokeAll(tasks)) f.get();
    }
    assertEquals(1, j.queryForObject("SELECT COUNT(*) FROM tb_seckill_request", Integer.class));
    assertEquals(9, SeckillStore.inventory(store.voucher(1, false)).available());
  }

  @Test
  void expiryAndConsumerRaceHasOneOutcome() throws Exception {
    tx.accept(r(7, "r1", 101), 1, false);
    j.update("UPDATE tb_seckill_request SET expires_at=?", LocalDateTime.now().minusSeconds(1));
    try (var pool = Executors.newFixedThreadPool(2)) {
      var a = pool.submit(() -> tx.complete(1, 101));
      var b = pool.submit(() -> tx.expire(1, 101));
      a.get();
      b.get();
    }
    assertEquals("FAILED", tx.result(1, 7, "r1").status());
    assertEquals(10, SeckillStore.inventory(store.voucher(1, false)).available());
    assertEquals(
        1,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_operation WHERE kind='RELEASE'", Integer.class));
  }

  @Test
  void redundantOrphanGetsIndependentIdempotentCleanup() {
    tx.accept(r(7, "r1", 101), 1, false);
    var extra = r(7, "r1", 102);
    tx.resolveOrphan(extra);
    tx.resolveOrphan(extra);
    assertEquals(
        1,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_id='redis:extra:102'",
            Integer.class));
    assertEquals(
        101L, j.queryForObject("SELECT order_id FROM tb_seckill_active_purchase", Long.class));
  }

  @Test
  void replayWithExtraOrderReleasesOnlyExtraHoldAndReturnsOriginalResult() {
    tx.fulfill(r(7, "same", 101), 1, false, 60);
    tx.cancel(1, 7, 101);
    tx.fulfill(r(7, "new", 103), 1, false, 60);
    var extra = r(7, "same", 102);
    assertEquals("CANCELLED", tx.fulfill(extra, 1, false, 60).status());
    tx.fulfill(extra, 1, false, 60);
    var payload =
        com.alibaba.fastjson.JSON.parseObject(
            j.queryForObject(
                "SELECT payload FROM tb_seckill_outbox WHERE event_id='redis:extra:102'",
                String.class));
    assertEquals("102", payload.getString("orderId"));
    assertEquals("RELEASE", payload.getString("kind"));
    assertEquals(
        103L, j.queryForObject("SELECT order_id FROM tb_seckill_active_purchase", Long.class));
    assertEquals(new Inventory(10, 9, 0, 1), store.inventory(store.voucher(1, false)));
  }

  @Test
  void orphanOfCommittedOrderCannotReleaseItsHold() {
    var held = r(7, "same", 101);
    tx.fulfill(held, 1, false, 60);
    tx.resolveOrphan(held);
    assertEquals(
        0,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REDIS' AND payload LIKE"
                + " '%RELEASE%'",
            Integer.class));
    assertEquals("SUCCEEDED", tx.result(1, 7, "same").status());
  }

  @Test
  void oldEpochExtraOrderCannotReleaseCurrentEpochStock() {
    tx.fulfill(r(7, "same", 101), 1, false, 60);
    j.update("UPDATE tb_seckill_voucher SET admission_epoch=2");
    assertEquals("101", tx.fulfill(r(7, "same", 102), 1, false, 60).orderId());
    assertEquals(
        0,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_id='redis:extra:102'",
            Integer.class));
  }

  @Test
  void processingSubscriptionIsNotReportedSuccessful() {
    tx.subscribe(1, 7, true);
    tx.accept(r(7, "auto_7_0", 101), 1, true);
    assertEquals(1, tx.subscribeStatus(1, 7));
    tx.complete(1, 101);
    assertEquals(2, tx.subscribeStatus(1, 7));
  }

  @Test
  void staleEpochCannotReserveDatabaseInventory() {
    j.update("UPDATE tb_seckill_voucher SET admission_epoch=2");
    assertThrows(SeckillFailure.class, () -> tx.accept(r(7, "r1", 101), 1, false));
    assertEquals(10, SeckillStore.inventory(store.voucher(1, false)).available());
  }

  @Test
  void requiredWriteFailureRollsBackCancellation() {
    tx.accept(r(7, "r1", 101), 1, false);
    tx.complete(1, 101);
    j.update("DELETE FROM tb_seckill_active_purchase");
    assertThrows(IllegalStateException.class, () -> tx.cancel(1, 7, 101));
    assertEquals("SUCCEEDED", tx.result(1, 7, "r1").status());
    assertEquals(1, j.queryForObject("SELECT status FROM tb_voucher_order", Integer.class));
    assertEquals(9, SeckillStore.inventory(store.voucher(1, false)).available());
  }

  @Test
  void messageCannotNameAnotherRequest() {
    tx.accept(r(7, "r1", 101), 1, false);
    assertThrows(SeckillFailure.class, () -> tx.completeEvent(1, 101, 8, "r1"));
    assertEquals("PROCESSING", tx.result(1, 7, "r1").status());
  }

  @Test
  void malformedRulesFailClosed() {
    assertFalse(SeckillTransactions.eligible(Map.of("allowed_levels", "1,bad"), 1));
    assertFalse(SeckillTransactions.eligible(Map.of("min_level", 11), 10));
  }

  @Test
  void cancelAllowsSubscriberToJoinQueueAgain() {
    tx.subscribe(1, 7, true);
    tx.accept(r(7, "auto_7_0", 101), 1, true);
    tx.complete(1, 101);
    tx.cancel(1, 7, 101);
    tx.subscribe(1, 7, true);
    assertEquals(
        "WAITING", j.queryForObject("SELECT status FROM tb_seckill_subscription", String.class));
    assertNull(j.queryForObject("SELECT order_id FROM tb_seckill_subscription", Long.class));
  }

  @Test
  void staleSubscriptionCandidateCannotConsumeNewQueuePosition() {
    tx.subscribe(1, 7, true);
    tx.subscribe(1, 7, false);
    tx.subscribe(1, 7, true);
    assertEquals("FAILED", tx.accept(r(7, "auto_7_0", 101), 1, true).status());
    assertEquals(
        "WAITING", j.queryForObject("SELECT status FROM tb_seckill_subscription", String.class));
    assertEquals(10, SeckillStore.inventory(store.voucher(1, false)).available());
  }

  @Test
  void adjustmentEventIdentityIncludesVoucher() {
    j.update(
        "INSERT INTO tb_seckill_voucher SELECT"
            + " 5,5,init_stock,stock,reserved_stock,sold_stock,version,rule_version,admission_epoch,admission_state,projection_seq,begin_time,end_time,allowed_levels,min_level,update_time"
            + " FROM tb_seckill_voucher WHERE voucher_id=1");
    tx.adjust(1, "same-client-id", 0, 11);
    tx.adjust(5, "same-client-id", 0, 11);
    assertEquals(
        2,
        j.queryForObject(
            "SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REDIS'", Integer.class));
  }
}
