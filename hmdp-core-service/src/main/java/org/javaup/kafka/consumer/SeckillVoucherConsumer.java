package org.javaup.kafka.consumer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.consumer.AbstractConsumerHandler;
import org.javaup.core.RedisKeyManage;
import org.javaup.core.SpringUtil;
import org.javaup.enums.BaseCode;
import org.javaup.enums.BusinessType;
import org.javaup.enums.LogType;
import org.javaup.enums.SeckillVoucherOrderOperate;
import org.javaup.exception.HmdpFrameException;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.kafka.producer.SeckillVoucherProducer;
import org.javaup.kafka.redis.RedisVoucherData;
import org.javaup.lua.SeckillShopTopBuyerOperate;
import org.javaup.message.MessageExtend;
import org.javaup.model.SeckillVoucherFullModel;
import org.javaup.redis.RedisCache;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.service.IAutoIssueNotifyService;
import org.javaup.service.ISeckillVoucherService;
import org.javaup.service.IVoucherOrderService;
import org.javaup.service.IVoucherReconcileLogService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;


/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: Kafka 消费者：处理秒杀券下单消息。
 * @author: 阿星不是程序员
 **/

@Slf4j
@Component
public class SeckillVoucherConsumer extends AbstractConsumerHandler<SeckillVoucherMessage> {
    
    public static Long MESSAGE_DELAY_TIME = 10000L;

    private static final int CONSUME_MAX_ATTEMPTS = 3;

    private static final long CONSUME_RETRY_BACKOFF_MILLIS = 200L;
    
    @Resource
    private IVoucherOrderService voucherOrderService;
    
    @Resource
    private RedisVoucherData redisVoucherData;
    
    @Resource
    private RedisCache redisCache;
    
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    
    @Resource
    private IVoucherReconcileLogService voucherReconcileLogService;
     
    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;
    
    
    @Resource
    private IAutoIssueNotifyService autoIssueNotifyService;

    @Resource
    private SeckillVoucherProducer seckillVoucherProducer;

    @Resource
    private SeckillShopTopBuyerOperate seckillShopTopBuyerOperate;
    
    
    public SeckillVoucherConsumer() {
        super(SeckillVoucherMessage.class);
    }
    
   
    @KafkaListener(
            topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME + "-" + SECKILL_VOUCHER_TOPIC}
    )
    public void onMessage(String value,
                          @Headers Map<String, Object> headers,
                          @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                          Acknowledgment acknowledgment) {
        MessageExtend<SeckillVoucherMessage> message = convert(value, toStringHeaders(headers));
        message.setKey(key);
        Throwable lastFailure = null;
        for (int attempt = 1; attempt <= CONSUME_MAX_ATTEMPTS; attempt++) {
            try {
                consume(message);
                lastFailure = null;
                break;
            } catch (Throwable throwable) {
                lastFailure = throwable;
                if (isNonRetryable(throwable) || attempt == CONSUME_MAX_ATTEMPTS) {
                    break;
                }
                log.warn("秒杀订单消费失败，准备重试，attempt={}, uuid={}, error={}",
                        attempt, message.getUuid(), throwable.getMessage());
                sleepBeforeRetry();
            }
        }
        if (lastFailure != null) {
            handleTerminalFailure(message, lastFailure);
        }
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
    
    @Override
    protected Boolean beforeConsume(MessageExtend<SeckillVoucherMessage> message) {
        long producerTimeTimestamp = message.getProducerTime().getTime();
        long delayTime = System.currentTimeMillis() - producerTimeTimestamp;
        // 延迟只用于告警。是否创建或补偿订单由持久化订单和预订状态决定。
        if (delayTime > MESSAGE_DELAY_TIME){
            log.warn("消费到Kafka秒杀消息延迟超过阈值，继续按订单状态处理，delay={}ms, orderId={}",
                    delayTime,message.getMessageBody().getOrderId());
        }
        return true;
    }
    
    @Override
    protected void doConsume(MessageExtend<SeckillVoucherMessage> message) {
        voucherOrderService.createVoucherOrderV2(message);
    }
    
    @Override
    protected void afterConsumeSuccess(MessageExtend<SeckillVoucherMessage> message) {
        super.afterConsumeSuccess(message);
        SeckillVoucherMessage messageBody = message.getMessageBody();
        Long userId = messageBody.getUserId();
        Long voucherId = messageBody.getVoucherId();
        Long orderId = messageBody.getOrderId();
        {
            try {
                RedisKeyBuild subscribeZSetKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SUBSCRIBE_ZSET_TAG_KEY,
                        messageBody.getVoucherId()
                );
                redisCache.delForSortedSet(subscribeZSetKey, String.valueOf(userId));
            } catch (Exception e) {
                throw new IllegalStateException("清理订阅ZSET成员失败", e);
            }
            if (Boolean.TRUE.equals(messageBody.getAutoIssue())) {
                try {
                    autoIssueNotifyService.sendAutoIssueNotify(voucherId, userId, orderId);
                } catch (Exception e) {
                    log.warn("自动发券通知发送失败，voucherId={}, userId={}, orderId={}, err={}",
                            voucherId, userId, orderId, e.getMessage());
                }
            }
            try {
                SeckillVoucherFullModel voucherFull = seckillVoucherService.queryByVoucherId(voucherId);
                if (Objects.isNull(voucherFull)) {
                    throw new IllegalStateException("秒杀优惠券不存在，无法更新店铺Top买家");
                }
                Long shopId = voucherFull.getShopId();
                // yyyyMMdd
                String day = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
                RedisKeyBuild dailyKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SHOP_TOP_BUYERS_DAILY_TAG_KEY,
                        shopId,
                        day
                );
                RedisKeyBuild dedupKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SHOP_TOP_BUYERS_ORDER_DEDUP_TAG_KEY,
                        shopId,
                        day
                );
                seckillShopTopBuyerOperate.execute(
                        dailyKey.getRelKey(), dedupKey.getRelKey(), orderId, userId,
                        TimeUnit.DAYS.toSeconds(90));
            } catch (Exception e) {
                throw new IllegalStateException("统计店铺Top买家失败", e);
            }
        }
    }
    
    @Override
    protected void afterConsumeFailure(final MessageExtend<SeckillVoucherMessage> message, 
                                       final Throwable throwable) {
        super.afterConsumeFailure(message, throwable);
    }

    private void handleTerminalFailure(final MessageExtend<SeckillVoucherMessage> message,
                                       final Throwable throwable) {
        SeckillVoucherOrderOperate seckillVoucherOrderOperate = SeckillVoucherOrderOperate.YES;
        if (throwable instanceof HmdpFrameException hmdpFrameException) {
            if (Objects.nonNull(hmdpFrameException.getCode()) && 
                    hmdpFrameException.getCode().equals(BaseCode.VOUCHER_ORDER_EXIST.getCode())){
                seckillVoucherOrderOperate = SeckillVoucherOrderOperate.NO;
            }
        }
        long traceId = snowflakeIdGenerator.nextId();
        redisVoucherData.rollbackRedisVoucherData(
                seckillVoucherOrderOperate,
                traceId,
                message.getMessageBody().getVoucherId(),
                message.getMessageBody().getUserId(),
                message.getMessageBody().getOrderId(),
                message.getMessageBody().getAfterQty(),
                message.getMessageBody().getChangeQty(),
                message.getMessageBody().getBeforeQty()
        );
        try {
            String detail = throwable == null ? "consume failed" : ("consume failed: " + throwable.getMessage());
            voucherReconcileLogService.saveReconcileLog(LogType.RESTORE.getCode(),
                    BusinessType.FAIL.getCode(), 
                    detail,
                    traceId,
                    message
            );
        } catch (Exception e) {
            log.warn("保存对账日志失败(消费失败)", e);
        }
        try {
            seckillVoucherProducer.sendMessageToDlq(
                    SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                    message,
                    throwable == null ? "consume failed" : throwable.getMessage()
            ).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("发送秒杀订单DLQ被中断", e);
        } catch (Exception e) {
            throw new IllegalStateException("发送秒杀订单DLQ失败", e);
        }
    }

    private boolean isNonRetryable(Throwable throwable) {
        return throwable instanceof HmdpFrameException hmdpFrameException
                && Objects.equals(hmdpFrameException.getCode(), BaseCode.VOUCHER_ORDER_EXIST.getCode());
    }

    private void sleepBeforeRetry() {
        try {
            TimeUnit.MILLISECONDS.sleep(CONSUME_RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("秒杀订单消费重试被中断", e);
        }
    }
}
