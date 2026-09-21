package org.javaup.cache;

import org.javaup.exception.HmdpFrameException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheRebuildGuardTest {

    @Test
    void shouldRejectRebuildWhenPermitIsExhaustedAndReleaseAfterwards() {
        CacheRebuildGuard guard = new CacheRebuildGuard(1);

        guard.execute(() -> {
            HmdpFrameException error = assertThrows(
                    HmdpFrameException.class,
                    () -> guard.execute(() -> "unreachable")
            );
            assertEquals("缓存重建繁忙，请稍后重试", error.getMessage());
            return null;
        });

        assertEquals("ok", guard.execute(() -> "ok"));
    }
}
