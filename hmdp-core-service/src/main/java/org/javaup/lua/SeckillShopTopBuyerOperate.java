package org.javaup.lua;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.javaup.redis.RedisCache;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.List;

/** Idempotently adds a successful order to a shop's daily buyer ranking. */
@Component
public class SeckillShopTopBuyerOperate {

    @Resource
    private RedisCache redisCache;

    private DefaultRedisScript<Long> redisScript;

    @PostConstruct
    public void init() {
        redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/seckillShopTopBuyer.lua")));
        redisScript.setResultType(Long.class);
    }

    public void execute(String rankingKey, String dedupKey, Long orderId, Long userId, long ttlSeconds) {
        redisCache.getInstance().execute(redisScript, List.of(rankingKey, dedupKey),
                String.valueOf(orderId), String.valueOf(userId), String.valueOf(ttlSeconds));
    }
}
