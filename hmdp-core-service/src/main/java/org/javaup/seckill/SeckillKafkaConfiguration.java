package org.javaup.seckill;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
public class SeckillKafkaConfiguration {
  @Bean
  @Primary
  public KafkaTemplate kafkaTemplate(KafkaProperties properties) {
    return new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(properties.buildProducerProperties()));
  }

  @Bean("seckillV2Template")
  public KafkaTemplate<String, String> seckillV2Template(KafkaProperties properties) {
    var config = new HashMap<>(properties.buildProducerProperties());
    config.put("key.serializer", StringSerializer.class);
    config.put("value.serializer", StringSerializer.class);
    config.put("max.block.ms", 3000);
    config.put("delivery.timeout.ms", 10000);
    config.put("request.timeout.ms", 5000);
    config.put("acks", "all");
    config.put("enable.idempotence", true);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
  }

  @Bean
  public KafkaAdmin.NewTopics seckillV2Topics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name("seckill-order-v2").partitions(4).replicas(1).build(),
        TopicBuilder.name("seckill-order-v2.DLT").partitions(4).replicas(1).build(),
        TopicBuilder.name(SeckillAdmissionPublisher.TOPIC).partitions(4).replicas(1).build(),
        TopicBuilder.name(SeckillAdmissionPublisher.TOPIC + ".DLT")
            .partitions(4)
            .replicas(1)
            .build());
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> seckillV2Factory(
      KafkaProperties properties,
      @Qualifier("seckillV2Template") KafkaTemplate<String, String> kafka) {
    var config = new HashMap<>(properties.buildConsumerProperties());
    config.put("key.deserializer", StringDeserializer.class);
    config.put("value.deserializer", StringDeserializer.class);
    config.put("enable.auto.commit", false);
    config.put("auto.offset.reset", "earliest");
    config.put("max.poll.records", 10);
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka, (record, e) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var backoff = new ExponentialBackOffWithMaxRetries(5);
    backoff.setInitialInterval(200);
    backoff.setMultiplier(2);
    backoff.setMaxInterval(3000);
    var handler = new DefaultErrorHandler(recoverer, backoff);
    handler.setCommitRecovered(true);
    factory.setCommonErrorHandler(handler);
    return factory;
  }

  @Bean("seckillLeaseScheduler")
  public ThreadPoolTaskScheduler leaseScheduler() {
    var scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.setThreadNamePrefix("seckill-lease-");
    return scheduler;
  }

  @Bean("seckillStreamScheduler")
  public ThreadPoolTaskScheduler streamScheduler() {
    var scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.setThreadNamePrefix("seckill-stream-");
    return scheduler;
  }

  @Bean("taskScheduler")
  public ThreadPoolTaskScheduler workerScheduler() {
    var scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(4);
    scheduler.setThreadNamePrefix("seckill-worker-");
    return scheduler;
  }

  @Bean
  public SeckillListener seckillV2Listener(SeckillTransactions tx) {
    return new SeckillListener(tx);
  }

  @Bean
  public QueuedListener seckillQueuedListener(SeckillQueuedProcessor processor) {
    return new QueuedListener(processor);
  }

  public static class QueuedListener {
    private final SeckillQueuedProcessor processor;

    public QueuedListener(SeckillQueuedProcessor processor) {
      this.processor = processor;
    }

    @KafkaListener(
        topics = SeckillAdmissionPublisher.TOPIC,
        groupId = "seckill-admission-v4",
        containerFactory = "seckillV2Factory",
        concurrency = "${seckill.v2.consumer-concurrency:1}",
        autoStartup = "${seckill.v2.workers-enabled:true}")
    public void listen(String raw, Acknowledgment ack) {
      var p = JSON.parseObject(raw);
      if (p == null
          || p.getIntValue("schemaVersion") != 4
          || !p.containsKey("autoIssue")
          || p.getJSONObject("reservation") == null)
        throw new IllegalArgumentException("INVALID_EVENT");
      var identity = p.getJSONObject("reservation");
      var r =
          new org.javaup.seckill.redis.RedisAdmissionGateway.Reservation(
              identity.getString("requestId"),
              identity.getString("orderId"),
              identity.getLongValue("voucherId"),
              identity.getLongValue("userId"),
              identity.getLongValue("epoch"),
              identity.getLongValue("ruleVersion"),
              identity.getLongValue("createdAt"),
              identity.getString("state"));
      if (r.voucherId() <= 0
          || r.userId() <= 0
          || r.epoch() < 1
          || r.createdAt() <= 0
          || r.requestId() == null
          || !r.requestId().matches("[A-Za-z0-9_-]{1,64}")
          || r.orderId() == null
          || !r.orderId().matches("[1-9][0-9]{0,18}")
          || !("admit:" + r.orderId()).equals(p.getString("eventId")))
        throw new IllegalArgumentException("INVALID_EVENT");
      processor.process(r, p.getBooleanValue("autoIssue"));
      ack.acknowledge();
    }
  }

  public static class SeckillListener {
    private final SeckillTransactions tx;

    public SeckillListener(SeckillTransactions tx) {
      this.tx = tx;
    }

    @KafkaListener(
        topics = "seckill-order-v2",
        groupId = "seckill-order-v2",
        containerFactory = "seckillV2Factory",
        autoStartup = "${seckill.v2.workers-enabled:true}")
    public void listen(String raw, Acknowledgment ack) {
      var p = JSON.parseObject(raw);
      if (p.getIntValue("schemaVersion") != 2
          || p.getLong("voucherId") == null
          || p.getLong("orderId") == null
          || p.getString("eventId") == null
          || p.getString("requestId") == null
          || p.getLong("userId") == null) throw new IllegalArgumentException("INVALID_EVENT");
      tx.completeEvent(
          p.getLongValue("voucherId"),
          p.getLongValue("orderId"),
          p.getLongValue("userId"),
          p.getString("requestId"));
      ack.acknowledge();
    }
  }
}
