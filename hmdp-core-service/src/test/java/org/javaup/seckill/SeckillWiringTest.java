package org.javaup.seckill;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.javaup.config.MvcConfig;
import org.javaup.context.DelayQueueContext;
import org.javaup.controller.VoucherController;
import org.javaup.controller.VoucherOrderController;
import org.javaup.execute.RateLimitHandler;
import org.javaup.kafka.producer.SeckillVoucherInvalidationProducer;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.javaup.service.IUserInfoService;
import org.javaup.service.IVoucherService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real Spring constructor/qualifier/AOP/listener wiring without external network clients. */
class SeckillWiringTest {
    @Configuration(proxyBeanMethods = false)
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    @EnableTransactionManagement
    @EnableKafka
    @Import({SeckillStore.class, SeckillTransactions.class, SeckillFacade.class,
            SeckillCatalogService.class, SeckillRecovery.class, SeckillMetrics.class,
            SeckillVoucherBloom.class, SeckillAdmissionCache.class, SeckillAdmissionPublisher.class, SeckillQueuedProcessor.class,
            SeckillSecurityInterceptor.class, RedisAdmissionGateway.class,
            SeckillKafkaConfiguration.class, SeckillWorkers.class,
            VoucherOrderController.class, VoucherController.class, MvcConfig.class,
            SeckillVoucherInvalidationProducer.class})
    static class WiringConfiguration {
        @Bean static PropertySourcesPlaceholderConfigurer placeholders() {
            return new PropertySourcesPlaceholderConfigurer();
        }
        @Bean org.javaup.handler.BloomFilterHandlerFactory bloomFilterHandlerFactory() { return mock(org.javaup.handler.BloomFilterHandlerFactory.class); }
        @Bean JdbcTemplate jdbcTemplate() { return mock(JdbcTemplate.class); }
        @Bean PlatformTransactionManager transactionManager() {
            return new DataSourceTransactionManager(new DriverManagerDataSource(
                    "jdbc:h2:mem:seckill_wiring;DB_CLOSE_DELAY=-1", "sa", ""));
        }
        @Bean StringRedisTemplate stringRedisTemplate() { return mock(StringRedisTemplate.class); }
        @Bean ObjectMapper objectMapper() { return new ObjectMapper(); }
        @Bean IUserInfoService userInfoService() { return mock(IUserInfoService.class); }
        @Bean IVoucherService voucherService() { return mock(IVoucherService.class); }
        @Bean SnowflakeIdGenerator snowflakeIdGenerator() { return mock(SnowflakeIdGenerator.class); }
        @Bean DelayQueueContext delayQueueContext() { return mock(DelayQueueContext.class); }
        @Bean RateLimitHandler rateLimitHandler() { return mock(RateLimitHandler.class); }
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
        @Bean KafkaProperties kafkaProperties() {
            KafkaProperties properties = new KafkaProperties();
            properties.setBootstrapServers(List.of("127.0.0.1:1"));
            properties.getProducer().setKeySerializer(StringSerializer.class);
            properties.getProducer().setValueSerializer(StringSerializer.class);
            return properties;
        }
    }

    @Test void defaultBuyerStatsNamespaceSurvivesAdmissionNamespaceUpgrade() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("safe-test",
                    java.util.Map.of("seckill.v2.workers-enabled", "false")));
            context.register(WiringConfiguration.class);
            context.refresh();
            var redis = context.getBean(StringRedisTemplate.class);
            doReturn("APPLIED").when(redis).execute(
                    org.mockito.ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<String>>any(),
                    anyList(), any(Object[].class));
            context.getBean(RedisAdmissionGateway.class).projectBuyerStats(9, "2026-09-28", 7, "101", "success:101", "SUCCESS");
            var call = mockingDetails(redis).getInvocations().stream()
                    .filter(invocation -> invocation.getMethod().getName().equals("execute")).findFirst().orElseThrow();
            assertEquals(List.of("hmdp:v2:buyers:{9:2026-09-28}:scores",
                    "hmdp:v2:buyers:{9:2026-09-28}:events", "hmdp:v2:buyers:{9:2026-09-28}:orders"), call.getArgument(1));
        }
    }

    @Test void productionBeansResolveQualifiersAndAopWithWorkersDisabled() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("safe-test",
                    java.util.Map.of("seckill.v2.workers-enabled", "false", "seckill.v2.activity-zone", "UTC")));
            context.register(WiringConfiguration.class);
            context.refresh();

            assertNotNull(context.getBean(VoucherOrderController.class));
            assertNotNull(context.getBean(VoucherController.class));
            assertNotNull(context.getBean(MvcConfig.class));
            assertNotNull(context.getBean(SeckillCatalogService.class));
            assertNotNull(context.getBean(SeckillRecovery.class));
            assertNotNull(context.getBean(SeckillVoucherInvalidationProducer.class));
            assertInstanceOf(DataSourceTransactionManager.class, context.getBean(PlatformTransactionManager.class));
            assertTrue(AopUtils.isAopProxy(context.getBean(SeckillFacade.class)));
            assertTrue(AopUtils.isAopProxy(context.getBean(SeckillTransactions.class)));
            assertTrue(AopUtils.isAopProxy(context.getBean(RedisAdmissionGateway.class)));

            KafkaTemplate<?, ?> primary = context.getBean(KafkaTemplate.class);
            KafkaTemplate<?, ?> v2 = context.getBean("seckillV2Template", KafkaTemplate.class);
            assertSame(context.getBean("kafkaTemplate"), primary);
            assertNotSame(primary, v2);
            assertSame(primary, ReflectionTestUtils.getField(
                    context.getBean(SeckillVoucherInvalidationProducer.class), "kafkaTemplate"));

            var registry = context.getBean(KafkaListenerEndpointRegistry.class);
            assertEquals(2, registry.getListenerContainers().size());
            registry.getListenerContainers().forEach(container -> assertFalse(container.isRunning()));
            assertTrue(context.getBeansOfType(SeckillWorkers.class).isEmpty());
            assertEquals(1, context.getBean("seckillLeaseScheduler", ThreadPoolTaskScheduler.class).getScheduledThreadPoolExecutor().getCorePoolSize());
            assertEquals(4, context.getBean("taskScheduler", ThreadPoolTaskScheduler.class).getScheduledThreadPoolExecutor().getCorePoolSize());

            // Constructor autowiring uses the actual @Qualifier, while conditional scan stays disabled.
            // Mockito JdbcTemplate returns empty voucher scans: a scheduled callback cannot reach DB.
            var factory = context.getAutowireCapableBeanFactory();
            SeckillWorkers worker = (SeckillWorkers) factory.autowire(
                    SeckillWorkers.class, AutowireCapableBeanFactory.AUTOWIRE_CONSTRUCTOR, false);
            assertSame(v2, ReflectionTestUtils.getField(worker, "kafka"));
            ScheduledAnnotationBeanPostProcessor scheduling = context.getBean(ScheduledAnnotationBeanPostProcessor.class);
            scheduling.postProcessAfterInitialization(worker, "isolatedWorkerWiringProbe");
            assertFalse(scheduling.getScheduledTasks().isEmpty());
            scheduling.postProcessBeforeDestruction(worker, "isolatedWorkerWiringProbe");
            var redisCalls = mockingDetails(context.getBean(StringRedisTemplate.class)).getInvocations();
            assertTrue(redisCalls.stream().allMatch(call ->
                    java.util.Set.of("setBeanClassLoader", "afterPropertiesSet").contains(call.getMethod().getName())),
                    "Only Spring lifecycle callbacks may touch the mocked Redis client");
            verifyNoInteractions(context.getBean(IUserInfoService.class));
        }
    }
}
