package org.javaup.kafka.producer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.core.RedisKeyManage;
import org.javaup.core.SpringUtil;
import org.javaup.entity.SeckillVoucher;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.service.ISeckillVoucherService;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;

/** Relays reservations written atomically with the Redis stock decrement to Kafka. */
@Slf4j
@Component
public class SeckillVoucherOutboxRelay {

    private static final int BATCH_SIZE = 100;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillVoucherProducer producer;

    @Resource
    private RedissonClient redissonClient;

    @Scheduled(fixedDelayString = "${seckill.outbox.relay-interval-ms:2000}")
    public void relay() {
        // Every voucher has its own Stream so Lua can write to the same Redis Cluster slot.
        List<SeckillVoucher> vouchers = seckillVoucherService.lambdaQuery()
                .select(SeckillVoucher::getVoucherId).list();
        for (SeckillVoucher voucher : vouchers) {
            try {
                relayVoucher(voucher.getVoucherId());
            } catch (Exception e) {
                log.error("秒杀预订投递失败，稍后重试，voucherId={}", voucher.getVoucherId(), e);
            }
        }
    }

    void relayVoucher(Long voucherId) {
        RLock lock = redissonClient.getLock("seckill:outbox:relay:" + voucherId);
        if (!lock.tryLock()) {
            return;
        }
        try {
            String outboxKey = RedisKeyBuild.createRedisKey(
                    RedisKeyManage.SECKILL_ORDER_OUTBOX_TAG_KEY, voucherId).getRelKey();
            String stateKey = RedisKeyBuild.createRedisKey(
                    RedisKeyManage.SECKILL_ORDER_STATE_TAG_KEY, voucherId).getRelKey();
            Range<String> window = Range.unbounded();
            while (true) {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream()
                        .range(outboxKey, window, Limit.limit().count(BATCH_SIZE));
                if (records == null || records.isEmpty()) {
                    return;
                }
                List<CompletableFuture<?>> deliveries = new ArrayList<>(records.size());
                for (MapRecord<String, Object, Object> record : records) {
                    Map<Object, Object> fields = record.getValue();
                    String orderId = String.valueOf(fields.get("orderId"));
                    String state = (String) stringRedisTemplate.opsForHash().get(stateKey, orderId);
                    if ("COMMITTED".equals(state) || "ROLLED_BACK".equals(state)
                            || "CANCELLED".equals(state)) {
                        stringRedisTemplate.opsForStream().delete(outboxKey, record.getId());
                        continue;
                    }
                    if (!"RESERVED".equals(state)) {
                        log.error("秒杀预订状态缺失，保留待投递消息供人工恢复，orderId={}", orderId);
                        continue;
                    }
                    SeckillVoucherMessage message = new SeckillVoucherMessage(
                            Long.valueOf(String.valueOf(fields.get("userId"))),
                            voucherId,
                            Long.valueOf(orderId),
                            Long.valueOf(String.valueOf(fields.get("traceId"))),
                            Integer.valueOf(String.valueOf(fields.get("beforeQty"))),
                            Integer.valueOf(String.valueOf(fields.get("changeQty"))),
                            Integer.valueOf(String.valueOf(fields.get("afterQty"))),
                            Boolean.valueOf(String.valueOf(fields.get("autoIssue"))));
                    // Delete only after broker acknowledgment. A crash between send and delete
                    // may resend, which createVoucherOrderV2 handles by orderId.
                    CompletableFuture<?> delivery = producer.sendPayload(
                            SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                            orderId, message, Collections.emptyMap()).handle((result, error) -> {
                                if (error == null) {
                                    stringRedisTemplate.opsForStream().delete(outboxKey, record.getId());
                                } else {
                                    log.warn("Kafka投递失败，保留Redis待投递记录，orderId={}", orderId, error);
                                }
                                return null;
                            });
                    deliveries.add(delivery);
                }
                CompletableFuture.allOf(deliveries.toArray(new CompletableFuture[0])).join();
                window = Range.rightUnbounded(
                        Range.Bound.exclusive(records.get(records.size() - 1).getId().getValue()));
                if (records.size() < BATCH_SIZE) {
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
