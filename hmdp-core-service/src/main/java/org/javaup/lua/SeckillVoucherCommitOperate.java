package org.javaup.lua;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.redis.RedisCache;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.List;

/** Atomically changes a Redis seckill reservation from RESERVED to COMMITTED. */
@Slf4j
@Component
public class SeckillVoucherCommitOperate {

    @Resource
    private RedisCache redisCache;

    private DefaultRedisScript<Long> redisScript;

    @PostConstruct
    public void init() {
        redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/seckillVoucherCommit.lua")));
        redisScript.setResultType(Long.class);
    }

    public int execute(String stateKey, Long orderId) {
        Object result = redisCache.getInstance().execute(
                redisScript, List.of(stateKey), String.valueOf(orderId));
        return result instanceof Number number ? number.intValue() : -1;
    }
}
