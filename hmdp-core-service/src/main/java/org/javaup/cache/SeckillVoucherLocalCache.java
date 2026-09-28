package org.javaup.cache;

import cn.hutool.core.date.LocalDateTimeUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.javaup.model.SeckillVoucherFullModel;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 本地缓存：秒杀优惠券详情
 * @author: 阿星不是程序员
 **/
@Component
public class SeckillVoucherLocalCache {

    private final Cache<String, SeckillVoucherFullModel> cache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfter(new Expiry<String, SeckillVoucherFullModel>() {
                @Override
                public long expireAfterCreate(String key, SeckillVoucherFullModel value, long currentTime) {
                    long ttlSeconds = 60L;
                    if (value != null && value.getEndTime() != null) {
                        ttlSeconds = Math.max(
                                LocalDateTimeUtil.between(LocalDateTimeUtil.now(), value.getEndTime()).getSeconds(),
                                1L
                        );
                    }
                    return TimeUnit.NANOSECONDS.convert(ttlSeconds, TimeUnit.SECONDS);
                }
                // 方法功能：按秒杀券结束时间计算本地缓存创建后的过期时间。
                
                @Override
                public long expireAfterUpdate(String key, SeckillVoucherFullModel value, long currentTime, long currentDuration) {
                    return currentDuration;
                }
                // 方法功能：保持秒杀券本地缓存更新前已有的过期时间。
                
                @Override
                public long expireAfterRead(String key, SeckillVoucherFullModel value, long currentTime, long currentDuration) {
                    return currentDuration;
                }
                // 方法功能：读取秒杀券本地缓存时不延长缓存过期时间。
            })
            .build();
    
    public SeckillVoucherFullModel get(String voucherId) {
        return cache.getIfPresent(voucherId);
    }
    // 方法功能：按请求参数查询并返回对应业务数据。
    
    public void put(String voucherId, SeckillVoucherFullModel voucher) {
        if (voucherId != null && voucher != null) {
            cache.put(voucherId, voucher);
        }
    }
    // 方法功能：写入指定秒杀券到本地缓存。
    
    public void invalidate(String voucherId) {
        cache.invalidate(voucherId);
    }
    // 方法功能：删除指定秒杀券的本地缓存。
}