package org.javaup.kafka.consumer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.consumer.AbstractConsumerHandler;
import org.javaup.core.RedisKeyManage;
import org.javaup.enums.BaseCode;
import org.javaup.enums.BusinessType;
import org.javaup.enums.LogType;
import org.javaup.enums.SeckillVoucherOrderOperate;
import org.javaup.exception.HmdpFrameException;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.kafka.redis.RedisVoucherData;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;


/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: Kafka 消费者：处理秒杀券下单消息。
 * @author: 阿星不是程序员
 **/

@Slf4j
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="seckill.legacy.enabled", havingValue="true")
@Component
public class SeckillVoucherConsumer extends AbstractConsumerHandler<SeckillVoucherMessage> {
    
    public static Long MESSAGE_DELAY_TIME = 10000L;
    
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


    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    private static final int EXECUTOR_THREADS = Math.max(2, CPU_CORES);
    private static final int EXECUTOR_QUEUE_CAPACITY = 1024 * Math.max(1, CPU_CORES);

    private static final ThreadPoolExecutor SECKILL_ORDER_CONSUME_TASK_EXECUTOR =
            new ThreadPoolExecutor(
                    EXECUTOR_THREADS,
                    EXECUTOR_THREADS,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(EXECUTOR_QUEUE_CAPACITY),
                    new NamedThreadFactory("seckill-order-consume-task", false),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

    private static class NamedThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final boolean daemon;
        private final AtomicInteger index = new AtomicInteger(1);

        public NamedThreadFactory(String namePrefix, boolean daemon) {
            this.namePrefix = namePrefix;
            this.daemon = daemon;
        }
        // 方法功能：实现 SeckillVoucherConsumer.NamedThreadFactory 的业务逻辑。

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + index.getAndIncrement());
            t.setDaemon(daemon);
            t.setUncaughtExceptionHandler((thread, ex) ->
                    log.error("未捕获异常，线程={}, err={}", thread.getName(), ex.getMessage(), ex)
            );
            return t;
        }
        // 方法功能：创建带业务前缀和守护标记的线程。
    }
    
    
    public SeckillVoucherConsumer() {
        super(SeckillVoucherMessage.class);
    }
    // 方法功能：初始化 SeckillVoucherConsumer 实例并设置必要依赖或父类参数。
    
   
    @KafkaListener(
            topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME + "-" + SECKILL_VOUCHER_TOPIC}
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
    protected Boolean beforeConsume(MessageExtend<SeckillVoucherMessage> message) {
        long producerTimeTimestamp = message.getProducerTime().getTime();
        long delayTime = System.currentTimeMillis() - producerTimeTimestamp;
        //如果消息超时时间达到了阈值（10秒）
        if (delayTime > MESSAGE_DELAY_TIME){
            log.info("消费到kafka的创建优惠券消息延迟时间大于了 {} 毫秒 此订单消息被丢弃 订单号 : {}",
                    delayTime,message.getMessageBody().getOrderId());
            long traceId = snowflakeIdGenerator.nextId();
            redisVoucherData.rollbackRedisVoucherData(
                    SeckillVoucherOrderOperate.YES,
                    traceId,
                    message.getMessageBody().getVoucherId(),
                    message.getMessageBody().getUserId(),
                    message.getMessageBody().getOrderId(),
                    // 这是回滚操作，所以redis中扣减前和扣减后的数量要和消息中的反过来
                    message.getMessageBody().getAfterQty(),
                    message.getMessageBody().getChangeQty(),
                    message.getMessageBody().getBeforeQty()
            );
            try {
                voucherReconcileLogService.saveReconcileLog(LogType.RESTORE.getCode(),
                        BusinessType.TIMEOUT.getCode(),
                        "message delayed " + delayTime + "ms, rollback redis",
                        traceId,
                        message);
            } catch (Exception e) {
                log.warn("保存对账日志失败(延迟丢弃)", e);
            }
            return false;
        }
        return true;
    }
    // 方法功能：在消费秒杀订单消息前执行幂等、状态和业务资格校验。
    
    @Override
    protected void doConsume(MessageExtend<SeckillVoucherMessage> message) {
        voucherOrderService.createVoucherOrderV2(message);
    }
    // 方法功能：执行消息消费的核心业务处理。
    
    @Override
    protected void afterConsumeSuccess(MessageExtend<SeckillVoucherMessage> message) {
        super.afterConsumeSuccess(message);
        SeckillVoucherMessage messageBody = message.getMessageBody();
        Long userId = messageBody.getUserId();
        Long voucherId = messageBody.getVoucherId();
        Long orderId = messageBody.getOrderId();
        SECKILL_ORDER_CONSUME_TASK_EXECUTOR.execute(() -> {
            try {
                RedisKeyBuild subscribeZSetKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SUBSCRIBE_ZSET_TAG_KEY,
                        messageBody.getVoucherId()
                );
                redisCache.delForSortedSet(subscribeZSetKey, String.valueOf(userId));
            } catch (Exception e) {
                log.warn("清理订阅ZSET成员失败，voucherId={}, userId={}, err={}", messageBody.getVoucherId(), userId, e.getMessage());
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
                    return;
                }
                Long shopId = voucherFull.getShopId();
                // yyyyMMdd
                String day = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
                RedisKeyBuild dailyKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SHOP_TOP_BUYERS_DAILY_TAG_KEY,
                        shopId,
                        day
                );
                redisCache.incrementScoreForSortedSet(dailyKey, String.valueOf(userId), 1.0);
                Long ttl = redisCache.getExpire(dailyKey, TimeUnit.SECONDS);
                if (ttl == null || ttl < 0) {
                    redisCache.expire(dailyKey, 90, TimeUnit.DAYS);
                }
            } catch (Exception e) {
                log.warn("统计店铺Top买家失败，忽略不影响主流程", e);
            }
        });
    }
    // 方法功能：处理消息消费成功后的订单落库、状态同步和对账记录。
    
    @Override
    protected void afterConsumeFailure(final MessageExtend<SeckillVoucherMessage> message, 
                                       final Throwable throwable) {
        super.afterConsumeFailure(message, throwable);
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
    }
    // 方法功能：处理消息消费失败后的指标记录、补偿或异常审计。
}
