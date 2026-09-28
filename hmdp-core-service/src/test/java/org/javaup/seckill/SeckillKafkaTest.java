package org.javaup.seckill;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Standalone KRaft broker on random ports. Never loads application.yml or production endpoints. */
@Timeout(value=60,unit=TimeUnit.SECONDS)
class SeckillKafkaTest {
    static EmbeddedKafkaKraftBroker broker;
    static KafkaProperties properties;
    static KafkaTemplate<String,String> template;
    static SeckillKafkaConfiguration config;
    static final String SUCCESS=SeckillAdmissionPublisher.TOPIC, FAIL="seckill-v2-retry-test";

    @BeforeAll static void startBroker() {
        broker=new EmbeddedKafkaKraftBroker(1,1,SUCCESS,FAIL,FAIL+".DLT");
        broker.brokerListProperty("seckill.tests.bootstrap");
        broker.brokerProperties(Map.of("group.initial.rebalance.delay.ms","0", "num.network.threads","2", "num.io.threads","2", "offsets.topic.replication.factor","1"));
        broker.afterPropertiesSet();
        properties=new KafkaProperties();properties.setBootstrapServers(List.of(broker.getBrokersAsString()));
        properties.getProducer().setAcks("all");
        properties.getConsumer().getProperties().put("session.timeout.ms","6000");
        config=new SeckillKafkaConfiguration();template=config.seckillV2Template(properties);
    }
    @AfterAll static void stopBroker() {
        if(template!=null)((DefaultKafkaProducerFactory<?,?>)template.getProducerFactory()).destroy();
        if(broker!=null)broker.destroy();
        System.clearProperty("seckill.tests.bootstrap");
    }

    @Test void realJsonWireAndRedeliveryAfterBusinessCommitBeforeAckCreateOneOrder() throws Exception {
        var ds=new DriverManagerDataSource("jdbc:h2:mem:kafka_"+UUID.randomUUID()+";MODE=MySQL;DB_CLOSE_DELAY=-1","sa","");
        var jdbc=new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE tb_seckill_voucher(voucher_id BIGINT PRIMARY KEY,init_stock INT,stock INT,reserved_stock INT,sold_stock INT,version BIGINT,rule_version BIGINT,admission_epoch BIGINT,admission_state VARCHAR(16),projection_seq BIGINT,begin_time TIMESTAMP,end_time TIMESTAMP,allowed_levels VARCHAR(64),min_level INT,update_time TIMESTAMP)");
        jdbc.execute("CREATE TABLE tb_voucher(id BIGINT PRIMARY KEY,status INT,shop_id BIGINT)");
        jdbc.execute("CREATE TABLE tb_voucher_order(id BIGINT PRIMARY KEY,user_id BIGINT,voucher_id BIGINT,status INT,create_time TIMESTAMP,update_time TIMESTAMP)");
        String ddl=Files.readString(Path.of("../sql/v2/new_tables.sql")).replace("___N__","").replaceAll("(?i) COLLATE utf8mb4_bin", "").replaceAll("(?i) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "");
        int index=0;for(String sql:ddl.split(";"))if(!sql.isBlank())jdbc.execute(sql.replaceAll("(?i)(KEY) `([a-z_]+)`","$1 `$2_"+(index++)+"`"));
        jdbc.update("INSERT INTO tb_seckill_voucher VALUES(1,10,10,0,0,0,0,1,'OPEN',0,?,?,NULL,NULL,CURRENT_TIMESTAMP)",LocalDateTime.now().minusHours(1),LocalDateTime.now().plusHours(1));
        jdbc.update("INSERT INTO tb_voucher VALUES(1,1,1)");
        var store=new SeckillStore(jdbc);var tx=new SeckillTransactions(store,new TransactionTemplate(new DataSourceTransactionManager(ds)),60);
        org.springframework.test.util.ReflectionTestUtils.setField(tx, "activityZone", "UTC");
        var users=mock(org.javaup.service.IUserInfoService.class);
        when(users.getByUserId(7L)).thenReturn(new org.javaup.entity.UserInfo().setLevel(1));
        var businessListener=new SeckillKafkaConfiguration.QueuedListener(new SeckillQueuedProcessor(tx,users,60));
        assertEquals("NOT_FOUND",tx.result(1,7,"wire-request").status());
        var attempts=new AtomicInteger();var completed=new CountDownLatch(1);var errors=new ConcurrentLinkedQueue<Throwable>();
        var factory=config.seckillV2Factory(properties,template);
        var container=factory.createContainer(SUCCESS);container.getContainerProperties().setGroupId("wire-"+UUID.randomUUID());
        container.setupMessageListener((AcknowledgingMessageListener<String,String>)(record,ack)->{
            try {
                assertEquals("1",record.key());
                var wire=JSON.parseObject(record.value());
                assertEquals(4,wire.getIntValue("schemaVersion"));
                assertEquals("admit:101",wire.getString("eventId"));
                var identity=wire.getJSONObject("reservation");
                assertEquals("wire-request",identity.getString("requestId"));
                assertEquals("101",identity.getString("orderId"));
                assertEquals(Set.of("requestId","orderId","voucherId","userId","epoch","ruleVersion","createdAt","state"),identity.keySet());
                int delivery=attempts.incrementAndGet();
                businessListener.listen(record.value(),()->{
                    if(delivery==1)throw new IllegalStateException("Injected failure after committed order before ACK");
                    ack.acknowledge();completed.countDown();
                });
            } catch(AssertionError e){errors.add(e);throw e;}
        });
        try {
            container.start();ContainerTestUtils.waitForAssignment(container,1);
            new SeckillAdmissionPublisher(template).publish(new Reservation("wire-request","101",1,7,1,0,System.currentTimeMillis(),"HELD"),false);
            assertTrue(completed.await(20,TimeUnit.SECONDS),"Redelivery must ACK within deadline");
            assertTrue(errors.isEmpty(),errors.toString());assertEquals(2,attempts.get());
            assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order",Integer.class));
            assertEquals(new Inventory(10,9,0,1),SeckillStore.inventory(store.voucher(1,false)));
            assertEquals("SUCCEEDED",tx.result(1,7,"wire-request").status());
            assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='CREATE'",Integer.class));
        } finally {container.stop();jdbc.execute("SHUTDOWN");}
    }

    @Test void initialDeliveryAndFiveRetriesPublishOriginalJsonToDlt() throws Exception {
        String payload=message("retry-request",202);var deliveries=new AtomicInteger();
        var factory=config.seckillV2Factory(properties,template);
        var container=factory.createContainer(FAIL);container.getContainerProperties().setGroupId("retry-"+UUID.randomUUID());
        container.setupMessageListener((AcknowledgingMessageListener<String,String>)(record,ack)->{deliveries.incrementAndGet();throw new IllegalStateException("Injected transient database outage");});
        var consumerProps=new HashMap<String,Object>();consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker.getBrokersAsString());consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,"dlt-"+UUID.randomUUID());consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        try(var dlt=new KafkaConsumer<String,String>(consumerProps,new StringDeserializer(),new StringDeserializer())) {
            dlt.subscribe(List.of(FAIL+".DLT"));container.start();ContainerTestUtils.waitForAssignment(container,1);
            template.send(FAIL,"1",payload).get(10,TimeUnit.SECONDS);
            ConsumerRecord<String,String> dead=null;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(25);
            while(dead==null&&System.nanoTime()<deadline){for(var record:dlt.poll(Duration.ofMillis(250)))dead=record;}
            assertNotNull(dead,"Retry exhaustion must reach the isolated DLT");assertEquals(payload,dead.value());assertEquals("1",dead.key());assertEquals(6,deliveries.get());
        } finally {container.stop();}
    }

    @Test @SuppressWarnings("unchecked") void failedDltSendThrowsAndDoesNotCommitOffset() {
        KafkaTemplate<Object,Object> broken=mock(KafkaTemplate.class);
        when(broken.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.<SendResult<Object,Object>>failedFuture(new IllegalStateException("DLT unavailable")));
        var recoverer=new DeadLetterPublishingRecoverer(broken,(record,e)->new TopicPartition(record.topic()+".DLT",record.partition()));
        recoverer.setVerifyPartition(false);recoverer.setFailIfSendResultIsError(true);recoverer.setWaitForSendResultTimeout(Duration.ofMillis(200));
        var handler=new DefaultErrorHandler(recoverer,new FixedBackOff(0,0));handler.setCommitRecovered(true);
        Consumer<String,String> consumer=mock(Consumer.class);var container=mock(MessageListenerContainer.class);
        var cp=new ContainerProperties("isolated");cp.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);when(container.getContainerProperties()).thenReturn(cp);
        var record=new ConsumerRecord<String,String>("isolated",0,0,"1",message("failed-dlt",303));
        assertThrows(RuntimeException.class,()->handler.handleRemaining(new IllegalStateException("DB unavailable"),List.of(record),consumer,container));
        verify(broken).send(any(ProducerRecord.class));
        assertTrue(mockingDetails(consumer).getInvocations().stream().noneMatch(invocation->invocation.getMethod().getName().startsWith("commit")),"A failed recovery must not commit an offset");
    }

    @Test @SuppressWarnings("unchecked") void producerFailureReturnsUnconfirmedRatherThanQueued() {
        KafkaTemplate<String,String> broken=mock(KafkaTemplate.class);
        when(broken.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var publisher=new SeckillAdmissionPublisher(broken);
        var failure=assertThrows(SeckillFailure.class,()->publisher.publish(new Reservation("r","101",1,7,1,0,System.currentTimeMillis(),"HELD"),false));
        assertEquals("DELIVERY_UNCONFIRMED",failure.getCode());
        assertEquals(503,failure.getHttpStatus());
    }

    @Test @SuppressWarnings("unchecked") void publisherWaitsForBrokerAcknowledgment() throws Exception {
        KafkaTemplate<String,String> delayed=mock(KafkaTemplate.class);
        var sent=new CountDownLatch(1);
        var acknowledgment=new CompletableFuture<SendResult<String,String>>();
        when(delayed.send(anyString(),anyString(),anyString())).thenAnswer(call->{sent.countDown();return acknowledgment;});
        var publisher=new SeckillAdmissionPublisher(delayed);
        var pool=Executors.newSingleThreadExecutor();
        try {
            var response=pool.submit(()->publisher.publish(new Reservation("r","101",1,7,1,0,System.currentTimeMillis(),"HELD"),false));
            assertTrue(sent.await(3,TimeUnit.SECONDS));
            assertFalse(response.isDone(),"Before broker ACK the HTTP boundary must remain unresolved");
            acknowledgment.complete(null);
            response.get(3,TimeUnit.SECONDS);
        } finally {pool.shutdownNow();}
    }

    @Test void oldSchemaAndWrongOrderEventIdentityAreRejectedBeforeBusinessProcessing() {
        var processor=mock(SeckillQueuedProcessor.class);
        var listener=new SeckillKafkaConfiguration.QueuedListener(processor);
        var wire=new com.alibaba.fastjson.JSONObject();
        wire.put("schemaVersion",3);wire.put("autoIssue",false);
        wire.put("eventId","admit:101");
        wire.put("reservation",Map.of("requestId","r","orderId","101","voucherId","1","userId","7","epoch","1","ruleVersion","0","createdAt",System.currentTimeMillis(),"state","HELD"));
        assertThrows(IllegalArgumentException.class,()->listener.listen(wire.toJSONString(),()->fail("Invalid message must not ACK")));
        wire.put("schemaVersion",4);wire.put("eventId","admit:102");
        assertThrows(IllegalArgumentException.class,()->listener.listen(wire.toJSONString(),()->fail("Invalid message must not ACK")));
        verifyNoInteractions(processor);
    }

    private static String message(String request,long order){return JSON.toJSONString(Map.of("schemaVersion",2,"voucherId","1","orderId",String.valueOf(order),"userId","7","requestId",request,"eventId","create:"+order));}
}
