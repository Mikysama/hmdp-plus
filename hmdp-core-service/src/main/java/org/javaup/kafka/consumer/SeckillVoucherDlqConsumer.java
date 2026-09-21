package org.javaup.kafka.consumer;

import com.alibaba.fastjson.JSON;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.consumer.AbstractConsumerHandler;
import org.javaup.entity.RollbackFailureLog;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.message.MessageExtend;
import org.javaup.service.IRollbackAlertService;
import org.javaup.service.IRollbackFailureLogService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.List;
import java.time.LocalDateTime;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;

/**
 * Records terminally failed seckill-order messages. Replaying a message must be an
 * explicit operation because its Redis reservation has already been rolled back.
 */
@Slf4j(topic = "AUDIT")
@Component
public class SeckillVoucherDlqConsumer extends AbstractConsumerHandler<SeckillVoucherMessage> {

    @Resource
    private IRollbackFailureLogService failureLogService;

    @Resource
    private IRollbackAlertService alertService;

    @Resource
    private SnowflakeIdGenerator idGenerator;

    @Resource
    private MeterRegistry meterRegistry;

    public SeckillVoucherDlqConsumer() {
        super(SeckillVoucherMessage.class);
    }

    @KafkaListener(
            topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME + "-" + SECKILL_VOUCHER_TOPIC + ".DLQ"},
            groupId = "${prefix.distinction.name:hmdp}-seckill-voucher-order-dlq-audit"
    )
    public void onMessage(String value, @Headers Map<String, Object> headers,
                          Acknowledgment acknowledgment) {
        consumeRaw(value, headers);
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    @Override
    protected void doConsume(MessageExtend<SeckillVoucherMessage> message) {
        SeckillVoucherMessage body = message.getMessageBody();
        List<RollbackFailureLog> existing = failureLogService.lambdaQuery()
                .eq(RollbackFailureLog::getOrderId, body.getOrderId())
                .eq(RollbackFailureLog::getSource, "seckill_order_dlq")
                .list();
        if (existing.isEmpty()) {
            String reason = message.getHeaders() == null ? "unknown"
                    : message.getHeaders().getOrDefault("dlqReason", "unknown");
            String detail = "DLQ待人工核查: " + reason;
            RollbackFailureLog incident = new RollbackFailureLog()
                    .setId(idGenerator.nextId())
                    .setOrderId(body.getOrderId())
                    .setUserId(body.getUserId())
                    .setVoucherId(body.getVoucherId())
                    .setTraceId(body.getTraceId())
                    .setSource("seckill_order_dlq")
                    .setDetail(detail.substring(0, Math.min(1024, detail.length())))
                    .setRetryAttempts(3)
                    .setCreateTime(LocalDateTime.now())
                    .setUpdateTime(LocalDateTime.now());
            if (!failureLogService.save(incident)) {
                throw new IllegalStateException("保存秒杀订单DLQ事件失败");
            }
            meterRegistry.counter("seckill_order_dlq_total").increment();
            alertService.sendRollbackAlert(incident);
        }
        log.error("SECKILL_ORDER_DLQ | uuid={} | reason={} | body={}",
                message.getUuid(),
                message.getHeaders() == null ? null : message.getHeaders().get("dlqReason"),
                JSON.toJSONString(message.getMessageBody()));
    }
}
