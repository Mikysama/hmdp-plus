package org.javaup.cache;

import jakarta.annotation.Resource;
import org.javaup.core.RedisKeyManage;
import org.javaup.kafka.message.SeckillVoucherInvalidationMessage;
import org.javaup.kafka.producer.SeckillVoucherInvalidationProducer;
import org.javaup.redis.RedisCache;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.core.SpringUtil;
import org.springframework.stereotype.Component;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_CACHE_INVALIDATION_TOPIC;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 业务发布入口：触发秒杀券缓存失效广播
 * @author: 阿星不是程序员
 **/
@Component
public class SeckillVoucherCacheInvalidationPublisher {

    @Resource
    private RedisCache redisCache;
    
    @Resource
    private SeckillVoucherInvalidationProducer invalidationProducer;
    
    @Resource
    private SeckillVoucherLocalCache seckillVoucherLocalCache;
    
    public void publishInvalidate(Long voucherId, String reason) {
        RedisKeyBuild seckillVoucherRedisKey =
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId);
        seckillVoucherLocalCache.invalidate(seckillVoucherRedisKey.getRelKey());
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId)); // 秒杀信息缓存删除
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId));  // 秒杀库存缓存删除
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_NULL_TAG_KEY, voucherId)); // 秒杀信息空缓存删除
        
        SeckillVoucherInvalidationMessage payload = new SeckillVoucherInvalidationMessage(voucherId, reason);
        invalidationProducer.sendPayload(
                SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_CACHE_INVALIDATION_TOPIC,
                payload
        );// 发送消息到Kafka，通知其他服务实例进行缓存失效
    }
    // 方法功能：删除指定秒杀券的本地和 Redis 缓存，并发送缓存失效广播消息。
}