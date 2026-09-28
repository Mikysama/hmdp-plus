package org.javaup.seckill;

import java.time.LocalDateTime;

public record SeckillResult(
    String requestId, String orderId, String status, String reasonCode, LocalDateTime expiresAt) {}
