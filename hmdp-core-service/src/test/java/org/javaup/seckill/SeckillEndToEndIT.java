package org.javaup.seckill;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.javaup.context.DelayQueueContext;
import org.javaup.entity.UserInfo;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.javaup.service.IUserInfoService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;
import java.net.ServerSocket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.atomic.LongAdder;
import static org.javaup.seckill.SeckillStore.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real two-instance MySQL + production ShardingSphere + isolated Redis/KRaft; no Boot context. */
@Timeout(value=90,unit=TimeUnit.SECONDS)
class SeckillEndToEndIT {
    static Process redisProcess; static LettuceConnectionFactory connection;
    static StringRedisTemplate redisTemplate; static RedisAdmissionGateway gateway;
    static EmbeddedKafkaKraftBroker broker; static KafkaTemplate<String,String> producer;
    static ConcurrentMessageListenerContainer<String,String> listener;
    static SeckillStore store; static SeckillTransactions tx; static SeckillFacade facade;
    static SeckillRecovery recovery; static SeckillWorkers workers;
    static SeckillVoucherBloom bloom; static org.redisson.api.RedissonClient bloomRedis;
    static final Set<Long> testVouchers=ConcurrentHashMap.newKeySet();
    static final String PREFIX="seckill-e2e";
    static final AtomicInteger dbAdmissions=new AtomicInteger();

    @BeforeEach void isolateWorkerScan() {
        // Each test owns its fixture vouchers. Load-test Outbox backlogs must not
        // starve a later lifecycle test after that test intentionally clears Redis.
        testVouchers.clear();
    }

    @BeforeAll static void setup() throws Exception {
        SeckillShardingIT.start();
        store=new SeckillStore(SeckillShardingIT.jdbc){
            @Override public List<Long> vouchers(long after,int count){return testVouchers.stream().filter(v->v>after).sorted().limit(count).toList();}
        };
        tx=new SeckillTransactions(store,new TransactionTemplate(new DataSourceTransactionManager(SeckillShardingIT.sharding)),300){
            @Override public SeckillResult accept(Reservation r,Integer level,boolean auto){dbAdmissions.incrementAndGet();return super.accept(r,level,auto);}
        };
        ReflectionTestUtils.setField(tx,"activityZone","UTC");
        int port;try(var socket=new ServerSocket(0)){port=socket.getLocalPort();}
        Path log=Files.createTempFile("seckill-e2e-redis-",".log");
        redisProcess=new ProcessBuilder("redis-server","--bind","127.0.0.1","--port",String.valueOf(port),"--save","","--appendonly","no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        connection=new LettuceConnectionFactory("127.0.0.1",port);connection.afterPropertiesSet();
        redisTemplate=new StringRedisTemplate(connection);redisTemplate.afterPropertiesSet();
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while(true){try(var c=connection.getConnection()){if("PONG".equals(c.ping()))break;}catch(Exception e){if(System.nanoTime()>deadline)throw e;Thread.sleep(50);}}
        gateway=new RedisAdmissionGateway(redisTemplate,new ObjectMapper(),PREFIX,PREFIX,100000,256);
        var bloomConfig = new org.redisson.config.Config();
        bloomConfig.setThreads(2).setNettyThreads(2);
        bloomConfig.useSingleServer().setAddress("redis://127.0.0.1:"+port).setConnectionMinimumIdleSize(1).setConnectionPoolSize(4);
        bloomRedis=org.redisson.Redisson.create(bloomConfig);
        org.javaup.handler.BloomFilterHandler handler;
        try(var prefix=mockStatic(org.javaup.core.SpringUtil.class)) {
            prefix.when(org.javaup.core.SpringUtil::getPrefixDistinctionName).thenReturn(PREFIX);
            handler=new org.javaup.handler.BloomFilterHandler(bloomRedis,"voucher-bloom",100000L,0.01);
        }
        var filters=mock(org.javaup.handler.BloomFilterHandlerFactory.class);
        when(filters.get(org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER)).thenReturn(handler);
        bloom=new SeckillVoucherBloom(filters,store,new SimpleMeterRegistry());
        recovery=new SeckillRecovery(store,tx,gateway);ReflectionTestUtils.setField(recovery,"activityZone","UTC");
        var users=mock(IUserInfoService.class);when(users.getByUserId(anyLong())).thenAnswer(call->new UserInfo().setUserId(call.getArgument(0)).setLevel(1));
        var ids=new SnowflakeIdGenerator(21,21);

        broker=new EmbeddedKafkaKraftBroker(1,1,SeckillAdmissionPublisher.TOPIC,SeckillAdmissionPublisher.TOPIC+".DLT");
        broker.brokerListProperty("seckill.e2e.bootstrap");broker.brokerProperties(Map.of("group.initial.rebalance.delay.ms","0","offsets.topic.num.partitions","1"));broker.afterPropertiesSet();
        var properties=new KafkaProperties();properties.setBootstrapServers(List.of(broker.getBrokersAsString()));
        var configuration=new SeckillKafkaConfiguration();producer=configuration.seckillV2Template(properties);
        facade=new SeckillFacade(gateway,tx,store,users,ids,new SeckillAdmissionCache(gateway),new SeckillAdmissionPublisher(producer),256);
        listener=configuration.seckillV2Factory(properties,producer).createContainer(SeckillAdmissionPublisher.TOPIC);listener.getContainerProperties().setGroupId("e2e-"+UUID.randomUUID());
        var business=configuration.seckillQueuedListener(new SeckillQueuedProcessor(tx,users,60));
        listener.setupMessageListener((AcknowledgingMessageListener<String,String>)(record,ack)->business.listen(record.value(),ack));listener.start();ContainerTestUtils.waitForAssignment(listener,1);
        var catalog=new SeckillCatalogService(store,tx,ids,mock(DelayQueueContext.class),120,bloom);
        workers=new SeckillWorkers(store,tx,gateway,recovery,facade,producer,new SimpleMeterRegistry(),catalog);
    }

    @AfterAll static void stop() throws Exception {
        if(listener!=null)listener.stop();
        if(producer!=null)((DefaultKafkaProducerFactory<?,?>)producer.getProducerFactory()).destroy();
        if(broker!=null)broker.destroy();
        if(bloomRedis!=null)bloomRedis.shutdown();
        if(connection!=null)connection.destroy();
        if(redisProcess!=null){redisProcess.destroy();if(!redisProcess.waitFor(5,TimeUnit.SECONDS))redisProcess.destroyForcibly();}
        System.clearProperty("seckill.e2e.bootstrap");SeckillShardingIT.stop();
    }

    @Test void databaseBloomLoadsAllShardsAndDetailsWithoutRedisAdmission() {
        for (long v=9200; v<9204; v++) {
            seed(v,2);
            redisTemplate.delete(PREFIX+":{"+v+"}:active");
        }
        assertEquals(List.of(9200L,9201L),store.catalogVoucherIds(9199,2));
        assertEquals(List.of(9202L,9203L),store.catalogVoucherIds(9201,2));
        assertTrue(bloom.initialize());
        var catalog=new SeckillCatalogService(store,tx,new SnowflakeIdGenerator(23,23),
                mock(DelayQueueContext.class),120,bloom);
        for (long v=9200; v<9204; v++) {
            assertFalse(redisTemplate.hasKey(PREFIX+":{"+v+"}:active"));
            assertEquals("e2e",catalog.get(v).get("title"));
        }
    }

    @Test void buyerHistorySurvivesChangingAdmissionNamespace() {
        String date="2026-09-28";
        gateway.projectBuyerStats(991, date, 7, "9001", "success:9001", "SUCCESS");
        var upgraded=new RedisAdmissionGateway(redisTemplate,new ObjectMapper(),PREFIX+"-next",PREFIX,100000,256);
        upgraded.projectBuyerStats(991,date,7,"9002","success:9002","SUCCESS");
        String scores=PREFIX+":buyers:{991:"+date+"}:scores";
        assertEquals(2.0,redisTemplate.opsForZSet().score(scores,"7"));
        upgraded.projectBuyerStats(991,date,7,"9001","cancel:9001","CANCEL");
        upgraded.projectBuyerStats(991,date,7,"9001","success:9001","SUCCESS");
        assertEquals(1.0,redisTemplate.opsForZSet().score(scores,"7"));
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(PREFIX+"-next:buyers:{991:"+date+"}:scores")));
    }

    static void seed(long voucher,int stock){
        testVouchers.add(voucher);
        for(String table:List.of("tb_seckill_notification","tb_seckill_recovery","tb_seckill_subscription","tb_seckill_request","tb_seckill_active_purchase","tb_seckill_outbox","tb_seckill_operation","tb_voucher_order","tb_seckill_voucher"))store.jdbc.update("DELETE FROM "+table+" WHERE voucher_id=?",voucher);
        store.jdbc.update("DELETE FROM tb_voucher WHERE id=?",voucher);
        store.jdbc.update("INSERT INTO tb_voucher(id,shop_id,title,pay_value,actual_value,type,status) VALUES(?,9,'e2e',100,100,1,1)",voucher);
        store.jdbc.update("INSERT INTO tb_seckill_voucher(id,voucher_id,init_stock,stock,reserved_stock,sold_stock,version,rule_version,admission_epoch,admission_state,projection_seq,begin_time,end_time) VALUES(?,?,?,?,0,0,0,0,0,'PAUSED',0,DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY),DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 1 DAY))",voucher,voucher,stock,stock);
        recovery.recover(voucher);
    }
    static SeckillResult submit(long v,long u,String request){workers.maintain();return facade.submit(v,u,request,gateway.issueToken(v,u),false);}
    static void settle(long v,String request,long user) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        do{workers.maintain();workers.dispatch();workers.dispatchKafka();if("SUCCEEDED".equals(tx.result(v,user,request).status())&&gateway.inspect(v).sequence()==number(store.voucher(v,false),"projection_seq"))return;Thread.sleep(50);}while(System.nanoTime()<deadline);
        fail("Full lifecycle did not converge: "+tx.result(v,user,request));
    }
    static void projectAll(long v) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        do{workers.maintain();workers.dispatch();if(gateway.inspect(v).sequence()==number(store.voucher(v,false),"projection_seq"))return;Thread.sleep(50);}while(System.nanoTime()<deadline);
        fail("Redis projection did not converge");
    }

    @Test void streamRelayCompletesOrderAfterHttpPublishFailureWithoutClientRetry() throws Exception {
        long v=9110,u=7110;seed(v,2);
        var failedPublisher=mock(SeckillAdmissionPublisher.class);
        doThrow(new SeckillFailure("DELIVERY_UNCONFIRMED",503)).when(failedPublisher).publish(any(),anyBoolean());
        var disconnected=new SeckillFacade(gateway,tx,store,mock(IUserInfoService.class),
            new SnowflakeIdGenerator(22,22),new SeckillAdmissionCache(gateway),failedPublisher,256);
        assertEquals("DELIVERY_UNCONFIRMED",assertThrows(SeckillFailure.class,
            ()->disconnected.submit(v,u,"stream-retry",gateway.issueToken(v,u),false)).getCode());
        assertEquals("NOT_FOUND",tx.result(v,u,"stream-retry").status());
        assertEquals(1,gateway.admissionOutbox(v,null,100).size());
        var held=gateway.findReservation(v,u,"stream-retry");
        var restarted=new SeckillAdmissionRelay(store,gateway,new SeckillAdmissionPublisher(producer),new SimpleMeterRegistry());
        restarted.relay(); // First successful delivery comes only from the background worker.
        settle(v,"stream-retry",u);
        // A second instance may have read the same entry before XDEL: replay stays idempotent.
        new SeckillAdmissionPublisher(producer).publish(held,false);
        assertEquals("SUCCEEDED",tx.fulfill(held,1,false,60).status());
        assertTrue(gateway.admissionOutbox(v,null,100).isEmpty());
        assertEquals(held.orderId(),tx.result(v,u,"stream-retry").orderId());
        assertEquals(1L,store.jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=?",Long.class,v));
        assertEquals(new Inventory(2,1,0,1),inventory(store.voucher(v,false)));
        recovery.audit(v);
    }

    @Test void streamRelayCannotResurrectAnExpiredOrphan() throws Exception {
        long v=9111,u=7111;seed(v,1);
        var held=gateway.reserve(v,u,"stream-expired",gateway.issueToken(v,u),"99111");
        tx.resolveOrphan(held);projectAll(v);
        var relay=new SeckillAdmissionRelay(store,gateway,new SeckillAdmissionPublisher(producer),new SimpleMeterRegistry());
        relay.relay();
        assertTrue(gateway.admissionOutbox(v,null,100).isEmpty());
        // Synchronous replay verifies the exact database path even if Kafka delivery is still queued.
        assertEquals("FAILED",tx.fulfill(held,1,false,60).status());
        assertEquals(0L,store.jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=?",Long.class,v));
        assertEquals(new Inventory(1,1,0,0),inventory(store.voucher(v,false)));
        recovery.audit(v);
    }

    @Test void kafkaAcknowledgedQueueDoesNotWriteMysqlUntilConsumption() throws Exception {
        long v=9104,u=7104;seed(v,2);listener.stop();
        try {
            var queued=submit(v,u,"pause-consumer");
            assertEquals("QUEUED",queued.status());
            assertEquals("NOT_FOUND",tx.result(v,u,"pause-consumer").status());
            assertEquals("PENDING",facade.result(v,u,"pause-consumer").status());
            assertEquals(new Inventory(2,2,0,0),inventory(store.voucher(v,false)));
            assertEquals(1,gateway.inspect(v).stock());
        } finally {listener.start();}
        settle(v,"pause-consumer",u);
        assertEquals(new Inventory(2,1,0,1),inventory(store.voucher(v,false)));
    }

    @Test void defaultSixtySecondLifetimeExpiresThroughTheRealDatabaseClock() throws Exception {
        long v=9103,u=7103;seed(v,1);
        var defaults=new SeckillTransactions(store,new TransactionTemplate(new DataSourceTransactionManager(SeckillShardingIT.sharding)),60);
        var held=gateway.reserve(v,u,"default-expiry",gateway.issueToken(v,u),"99103");
        defaults.accept(held,1,false);
        Long seconds=store.jdbc.queryForObject("SELECT TIMESTAMPDIFF(SECOND,create_time,expires_at) FROM tb_seckill_request WHERE voucher_id=? AND id=?",Long.class,v,99103L);
        assertTrue(seconds>=59&&seconds<=60,"Production lifetime must be 60 seconds");
        // Move only this test fixture timestamp forward logically; no one-minute sleep needed.
        store.jdbc.update("UPDATE tb_seckill_request SET expires_at=DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 SECOND) WHERE voucher_id=? AND id=?",v,99103L);
        defaults.expire(v,99103L);projectAll(v);
        assertEquals("FAILED",defaults.result(v,u,"default-expiry").status());
        assertEquals(new Inventory(1,1,0,0),inventory(store.voucher(v,false)));
        assertEquals(1,gateway.inspect(v).stock());
    }

    @Test void reservationOutboxKafkaCancellationReplayAndRebuild() throws Exception {
        long v=9100,u=7100;seed(v,2);
        var result=submit(v,u,"full-cycle");assertEquals("QUEUED",result.status());assertEquals(1,gateway.inspect(v).stock());
        settle(v,"full-cycle",u);assertEquals(new Inventory(2,1,0,1),inventory(store.voucher(v,false)));
        var oldEvents=store.jdbc.queryForList("SELECT * FROM tb_seckill_outbox WHERE voucher_id=? AND event_type='REDIS' ORDER BY sequence_no",v);
        tx.cancel(v,u,Long.parseLong(result.orderId()));tx.cancel(v,u,Long.parseLong(result.orderId()));projectAll(v);
        assertEquals(2,gateway.inspect(v).stock());assertEquals(new Inventory(2,2,0,0),inventory(store.voucher(v,false)));
        var second=submit(v,u,"repurchase");settle(v,"repurchase",u);
        for(var event:oldEvents)workers.deliver(event);
        assertEquals(second.orderId(),String.valueOf(store.jdbc.queryForObject("SELECT order_id FROM tb_seckill_active_purchase WHERE voucher_id=? AND user_id=?",Long.class,v,u)));
        long epoch=gateway.inspect(v).epoch();recovery.recover(v);assertTrue(gateway.inspect(v).epoch()>epoch);
        for(var event:oldEvents)workers.deliver(event);
        recovery.audit(v);assertEquals(1,gateway.inspect(v).stock());
    }

    @Test void redisLossDoesNotRollBackCommittedAcceptanceAndResponseLossRetriesSameRequest() throws Exception {
        long v=9101,u=7101;seed(v,3);
        var accepted=submit(v,u,"response-lost");
        var same=submit(v,u,"response-lost");assertEquals(accepted.orderId(),same.orderId());
        settle(v,"response-lost",u);assertEquals(2,inventory(store.voucher(v,false)).available());
        try(var c=connection.getConnection()){c.serverCommands().flushDb();}
        assertThrows(SeckillFailure.class,()->submit(v,7102,"while-redis-empty"));
        // Accepted database work completes even before Redis admission is recovered.
        tx.complete(v,Long.parseLong(accepted.orderId()));
        recovery.recover(v);projectAll(v);recovery.audit(v);
        assertEquals("QUEUED",submit(v,u,"response-lost").status());assertEquals("SUCCEEDED",facade.result(v,u,"response-lost").status());assertEquals(2,gateway.inspect(v).stock());
    }

    @Test void orphanTerminationBlocksLateAcceptanceAndRestoresOneQualification() throws Exception {
        long v=9102,u=7102;seed(v,1);
        var held=gateway.reserve(v,u,"orphan",gateway.issueToken(v,u),"99102");
        tx.resolveOrphan(held);projectAll(v);
        assertEquals("FAILED",tx.accept(held,1,false).status());assertEquals(1,gateway.inspect(v).stock());
        assertEquals(0L,store.jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_active_purchase WHERE voucher_id=?",Long.class,v));
        recovery.audit(v);
    }

    /** Opt-in queue filter test. Consumer is stopped to prove zero synchronous MySQL writes. */
    @Test @EnabledIfSystemProperty(named="seckill.test.load",matches="true")
    @Timeout(value=480,unit=TimeUnit.SECONDS)
    void boundedQueueFilteringKeepsDatabaseUntouchedUntilConsumerStarts() throws Exception {
        long v=9200;seed(v,100);listener.stop();
        var renewal=Executors.newSingleThreadScheduledExecutor();
        long epoch=gateway.activeEpoch(v);
        renewal.scheduleAtFixedRate(()->gateway.renew(v,epoch),0,1,TimeUnit.SECONDS);
        int requests=Integer.getInteger("seckill.test.requests",100000);
        var next=new AtomicInteger();var queued=new AtomicInteger();
        long started=System.nanoTime();
        try(var pool=Executors.newFixedThreadPool(32)) {
            var tasks=new ArrayList<Callable<Void>>();
            for(int t=0;t<32;t++)tasks.add(()->{int index;while((index=next.getAndIncrement())<requests){
                long user=1000000L+index;
                try {var result=facade.submit(v,user,"queue-"+index,gateway.issueToken(v,user),false);
                    if("QUEUED".equals(result.status()))queued.incrementAndGet();
                }catch(SeckillFailure failure){assertTrue(Set.of("SOLD_OUT","ADMISSION_BUSY","RATE_LIMITED").contains(failure.getCode()),failure.getCode());}
            }return null;});
            for(var result:pool.invokeAll(tasks))result.get();
            assertEquals(100,queued.get());
            assertEquals(new Inventory(100,100,0,0),inventory(store.voucher(v,false)));
            assertEquals(0L,store.jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_request WHERE voucher_id=?",Long.class,v));
            Path output=Path.of("../docs/seckill-v2/results/queue-filter-"+java.time.Instant.now().toString().replace(':','-')+".csv");
            Files.createDirectories(output.getParent());
            Files.writeString(output,"requests,concurrency,stock,queued,db_requests_before_consume,elapsed_ms\n"+requests+",32,100,"+queued.get()+",0,"+(System.nanoTime()-started)/1e6+"\n");
        } finally {renewal.shutdownNow();listener.start();}
    }
}
