package org.javaup.seckill;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.springframework.stereotype.Component;

/** L1 activity hints, loaded only from Redis. Lua remains authoritative for admission. */
@Component
public class SeckillAdmissionCache {
  private final RedisAdmissionGateway redis;
  private final SeckillVoucherBloom bloom;
  private final Cache<Long, RedisAdmissionGateway.Activity> activities =
      Caffeine.newBuilder().maximumSize(10000).expireAfterWrite(Duration.ofSeconds(1)).build();

  public SeckillAdmissionCache(RedisAdmissionGateway redis, SeckillVoucherBloom bloom) {
    this.redis = redis;
    this.bloom = bloom;
  }

  public void checkExists(long voucher) {
    bloom.check(voucher);
  }

  public void check(long voucher, long user, String request) {
    var activity =
        activities.get(
            voucher,
            id -> {
              bloom.check(id);
              return redis.activity(id);
            });
    long now = System.currentTimeMillis();
    String reason =
        !"1".equals(activity.status()) && !"ACTIVE".equals(activity.status())
            ? "INACTIVE"
            : now < activity.beginMillis()
                ? "NOT_STARTED"
                : now >= activity.endMillis() ? "ENDED" : null;
    // Existing identities must remain retryable after the activity closes.
    if (reason != null && redis.findReservation(voucher, user, request) == null)
      throw new SeckillFailure(reason, 409);
  }
}
