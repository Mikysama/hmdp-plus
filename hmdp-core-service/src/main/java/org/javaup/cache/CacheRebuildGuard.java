package org.javaup.cache;

import org.javaup.exception.HmdpFrameException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Per-instance bulkhead for database cache rebuilds. Distributed locks protect
 * one key; this guard also limits rebuilds for many different keys.
 */
@Component
public class CacheRebuildGuard {

    private final Semaphore permits;

    public CacheRebuildGuard(@Value("${cache.rebuild.max-concurrent:50}") int maxConcurrent) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("cache.rebuild.max-concurrent must be greater than zero");
        }
        this.permits = new Semaphore(maxConcurrent);
    }

    public <T> T execute(Supplier<T> databaseQuery) {
        if (!permits.tryAcquire()) {
            throw new HmdpFrameException(429, "缓存重建繁忙，请稍后重试");
        }
        try {
            return databaseQuery.get();
        } finally {
            permits.release();
        }
    }
}
