package org.javaup.seckill;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Requires explicitly provided disposable MySQL URLs; never boots application initializers. */
class SeckillShardingIT {
    static HikariDataSource[] physical = new HikariDataSource[2];
    static DataSource sharding;
    static JdbcTemplate jdbc;
    static SeckillStore store;
    static SeckillTransactions transactions;

    @BeforeAll static void start() throws Exception {
        String first = System.getProperty("seckill.test.mysql0");
        String second = System.getProperty("seckill.test.mysql1");
        Assumptions.assumeTrue(first != null && second != null,
                "Use scripts/run-seckill-v2-mysql-tests.sh; no default database is allowed");
        assertNotEquals(first, second, "Use independent MySQL instances");
        Map<String, DataSource> sources = new LinkedHashMap<>();
        for (int index = 0; index < 2; index++) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(index == 0 ? first : second);
            config.setUsername(System.getProperty("seckill.test.mysqlUser", "root"));
            config.setPassword(System.getProperty("seckill.test.mysqlPassword", ""));
            config.setMaximumPoolSize(8);
            config.setMinimumIdle(1);
            config.setConnectionInitSql("SET time_zone = '+00:00'");
            physical[index] = new HikariDataSource(config);
            sources.put("ds_" + index, physical[index]);
        }
        String uuid0 = new JdbcTemplate(physical[0]).queryForObject("SELECT @@server_uuid", String.class);
        String uuid1 = new JdbcTemplate(physical[1]).queryForObject("SELECT @@server_uuid", String.class);
        assertNotEquals(uuid0, uuid1, "Two schemas on the same MySQL instance are insufficient");
        String original;
        try (var input = SeckillShardingIT.class.getResourceAsStream("/shardingsphere.yaml")) {
            assertNotNull(input);
            original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        // External map supplies only isolated URLs; production dataSources are never parsed.
        String rules = original.substring(original.indexOf("rules:"));
        rules = rules.replace("sql-show: true", "sql-show: false");
        sharding = YamlShardingSphereDataSourceFactory.createDataSource(sources, rules.getBytes(StandardCharsets.UTF_8));
        jdbc = new JdbcTemplate(sharding);
        store = new SeckillStore(jdbc);
        transactions = new SeckillTransactions(store,
                new TransactionTemplate(new DataSourceTransactionManager(sharding)), 60);
    }

    @AfterAll static void stop() throws Exception {
        if (sharding instanceof AutoCloseable closeable) closeable.close();
        for (HikariDataSource source : physical) if (source != null) source.close();
    }

    void seed(long voucher) {
        for (String table : new String[]{"tb_seckill_request", "tb_seckill_active_purchase", "tb_seckill_outbox", "tb_seckill_operation", "tb_voucher_order", "tb_seckill_voucher"})
            jdbc.update("DELETE FROM " + table + " WHERE voucher_id=?", voucher);
        jdbc.update("DELETE FROM tb_voucher WHERE id=?", voucher);
        jdbc.update("INSERT INTO tb_voucher(id,shop_id,title,pay_value,actual_value,type,status) VALUES(?,9,'integration',100,100,1,1)", voucher);
        jdbc.update("INSERT INTO tb_seckill_voucher(id,voucher_id,init_stock,stock,reserved_stock,sold_stock,version,rule_version,admission_epoch,admission_state,projection_seq,begin_time,end_time) " +
                        "VALUES(?,?,10,10,0,0,0,0,1,'OPEN',0,DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY),DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 1 DAY))", voucher, voucher);
    }

    Reservation reservation(long voucher, long order) {
        // Deliberately choose user/order routes different from voucher's old routes.
        return new Reservation("request-" + order, String.valueOf(order),
                voucher, voucher + 1, 1, 0, System.currentTimeMillis(), "HELD");
    }

    @Test void catalogBloomScanPaginatesAcrossAllFourPhysicalRoutes() {
        for (long voucher=8300; voucher<8304; voucher++) seed(voucher);
        assertEquals(java.util.List.of(8300L,8301L),store.catalogVoucherIds(8299,2));
        assertEquals(java.util.List.of(8302L,8303L),store.catalogVoucherIds(8301,2));
    }

    @Test void successfulLifecycleStaysOnOnePhysicalShardForAllFourRoutes() {
        for (long voucher = 8400; voucher < 8404; voucher++) {
            long order = voucher + 101;
            seed(voucher);
            assertEquals("PROCESSING", transactions.accept(reservation(voucher, order), 1, false).status());
            assertEquals("SUCCEEDED", transactions.complete(voucher, order).status());
            for (String table : new String[]{"tb_seckill_voucher", "tb_seckill_request", "tb_seckill_active_purchase",
                    "tb_voucher_order", "tb_seckill_operation", "tb_seckill_outbox"}) {
                long total = 0;
                for (int db = 0; db < 2; db++) for (int suffix = 0; suffix < 2; suffix++) {
                    long rows = new JdbcTemplate(physical[db]).queryForObject(
                            "SELECT COUNT(*) FROM " + table + "_" + suffix + " WHERE voucher_id=?", Long.class, voucher);
                    boolean expected = db == voucher % 2 && suffix == (voucher / 2) % 2;
                    if (!expected) assertEquals(0, rows, table + " routed to wrong node");
                    total += rows;
                }
                assertTrue(total > 0, table + " must persist records");
            }
            assertEquals("CANCELLED", transactions.cancel(voucher, voucher + 1, order).status());
            assertEquals("CANCELLED", transactions.cancel(voucher, voucher + 1, order).status());
            assertEquals(new Inventory(10, 10, 0, 0), SeckillStore.inventory(store.voucher(voucher, false)));
        }
        assertTrue(store.vouchers(8399, 100).containsAll(java.util.List.of(8400L, 8401L, 8402L, 8403L)));
    }

    @Test void outerRollbackRevertsInventoryRequestRelationshipAndOutboxTogether() {
        long voucher = 8503, order = 18504;
        seed(voucher);
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(sharding));
        assertThrows(IllegalStateException.class, () -> transaction.execute(status -> {
            transactions.accept(reservation(voucher, order), 1, false);
            throw new IllegalStateException("injected after Outbox write before commit");
        }));
        assertEquals(new Inventory(10, 10, 0, 0), SeckillStore.inventory(store.voucher(voucher, false)));
        assertEquals("NOT_FOUND", transactions.result(voucher, voucher + 1, "request-" + order).status());
        for (String table : new String[]{"tb_seckill_request", "tb_seckill_active_purchase", "tb_seckill_outbox", "tb_seckill_operation"}) {
            assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE voucher_id=?", Long.class, voucher));
        }
    }

    @Test void reconciliationJoinsStayWithinVoucherAndHandleCrossShardScan() {
        long voucher = 8602, order = 8603;
        seed(voucher);
        transactions.accept(reservation(voucher, order), 1, false);
        transactions.complete(voucher, order);
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_active_purchase a LEFT JOIN tb_seckill_request r ON a.voucher_id=r.voucher_id AND a.order_id=r.id WHERE a.voucher_id=? AND (r.id IS NULL OR a.user_id<>r.user_id OR r.status NOT IN ('PROCESSING','SUCCEEDED'))", Long.class, voucher));
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_request r LEFT JOIN tb_seckill_outbox e ON r.voucher_id=e.voucher_id AND e.event_id=CONCAT('create:',r.id) WHERE r.voucher_id=? AND r.status='PROCESSING' AND e.event_id IS NULL", Long.class, voucher));
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order o LEFT JOIN tb_seckill_request r ON o.voucher_id=r.voucher_id AND o.id=r.id WHERE o.voucher_id=? AND o.status=1 AND (r.id IS NULL OR r.status<>'SUCCEEDED' OR r.user_id<>o.user_id)", Long.class, voucher));
        assertTrue(store.vouchers(8601, 100).contains(voucher));
    }

    @Test void publicShopVoucherJoinReturnsEachVoucherOnceWithItsStock() {
        for (long voucher = 8700; voucher < 8704; voucher++) {
            seed(voucher);
            jdbc.update("UPDATE tb_voucher SET shop_id=8700 WHERE id=?", voucher);
        }
        var rows = jdbc.queryForList("SELECT v.id,v.shop_id,sv.stock FROM tb_voucher v LEFT JOIN tb_seckill_voucher sv ON v.id=sv.voucher_id WHERE v.shop_id=? AND v.status=1", 8700);
        assertEquals(4, rows.size(), "Public list JOIN must not multiply physical table combinations");
        for (var row : rows) assertEquals(10, ((Number) row.get("stock")).intValue());
    }
}
