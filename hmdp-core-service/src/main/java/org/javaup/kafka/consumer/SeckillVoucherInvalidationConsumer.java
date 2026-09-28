package org.javaup.kafka.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.cache.SeckillVoucherLocalCache;
import org.javaup.consumer.AbstractConsumerHandler;
import org.javaup.core.RedisKeyManage;
import org.javaup.kafka.message.SeckillVoucherInvalidationMessage;
import org.javaup.message.MessageExtend;
import org.javaup.redis.RedisCache;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.servicelock.LockType;
import org.javaup.servicelock.annotion.ServiceLock;
import org.springframework.aop.framework.AopContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_CACHE_INVALIDATION_TOPIC;
import static org.javaup.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;
import static org.javaup.constant.DistributedLockConstants.UPDATE_SECKILL_VOUCHER_LOCK;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: Kafka 消费者：接收“秒杀券缓存失效”广播
 * @author: 阿星不是程序员
 **/
@Slf4j
@Component
public class SeckillVoucherInvalidationConsumer extends AbstractConsumerHandler<SeckillVoucherInvalidationMessage> {


    @Resource
    private SeckillVoucherLocalCache seckillVoucherLocalCache;
    
    @Resource
    private MeterRegistry meterRegistry;


    @Resource
    private RedisCache redisCache;

    public SeckillVoucherInvalidationConsumer() {
        super(SeckillVoucherInvalidationMessage.class);
    }
    // 方法功能：初始化 SeckillVoucherInvalidationConsumer 实例并设置必要依赖或父类参数。
    
    @KafkaListener(
            topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME + "-" + SECKILL_VOUCHER_CACHE_INVALIDATION_TOPIC},
            groupId = "${prefix.distinction.name:hmdp}-seckill_voucher_cache_invalidation-${random.uuid}"
    )
    public void onMessage(String value,
                          @Headers Map<String, Object> headers,
                          @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                          Acknowledgment acknowledgment) {
        consumeRaw(value, key, headers);
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
    // 方法功能：接收 Kafka 原始消息，委托统一消费流程处理并提交确认。
    
    @Override
    protected void doConsume(MessageExtend<SeckillVoucherInvalidationMessage> message) {
        SeckillVoucherInvalidationMessage body = message.getMessageBody();
        if (Objects.isNull(body.getVoucherId())) {
            log.warn("收到缓存失效消息但载荷为空或voucherId缺失, uuid={}", message.getUuid());
            return;
        }
        Long voucherId = body.getVoucherId();
        
        ((SeckillVoucherInvalidationConsumer) AopContext.currentProxy()).delCache(voucherId);
    }
    // 方法功能：执行消息消费的核心业务处理。
    
    @ServiceLock(lockType= LockType.Write,name = UPDATE_SECKILL_VOUCHER_LOCK,keys = {"#voucherId"})
    public void delCache(Long voucherId){
        RedisKeyBuild seckillVoucherRedisKey =
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId);
        seckillVoucherLocalCache.invalidate(seckillVoucherRedisKey.getRelKey());
        
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId));
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId));
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_NULL_TAG_KEY, voucherId));
        
    }
    // 方法功能：删除指定秒杀券的本地缓存和 Redis 缓存。
    
    @Override
    protected void afterConsumeFailure(final MessageExtend<SeckillVoucherInvalidationMessage> message, final Throwable throwable) {
        super.afterConsumeFailure(message, throwable);
        log.warn("删除Redis缓存失败 voucherId={}", message.getMessageBody().getVoucherId(), throwable);
        safeInc(errorTag(throwable));
    }
    // 方法功能：处理消息消费失败后的指标记录、补偿或异常审计。
    
    private void safeInc(String tagValue) {
        try {
            if (meterRegistry != null) {
                meterRegistry.counter("seckill_invalidation_consume_failures", "error", tagValue).increment();
            }
        } catch (Exception ignore) {
        }
    }
    // 方法功能：安全递增监控指标，避免指标异常影响主流程。

    private String errorTag(Throwable t) {
        return t == null ? "unknown" : t.getClass().getSimpleName();
    }
    // 方法功能：将异常类型转换为监控标签。
}
