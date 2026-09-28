package org.javaup.seckill;

import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.javaup.service.IUserInfoService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Runs only downstream of Kafka; membership I/O stays outside the local transaction. */
@Service
public class SeckillQueuedProcessor {
  private final SeckillTransactions tx;
  private final IUserInfoService users;
  private final int queueSeconds;

  public SeckillQueuedProcessor(
      SeckillTransactions tx,
      IUserInfoService users,
      @Value("${seckill.v2.queue-seconds:60}") int queueSeconds) {
    if (queueSeconds < 1) throw new IllegalArgumentException("Positive queue lifetime required");
    this.tx = tx;
    this.users = users;
    this.queueSeconds = queueSeconds;
  }

  public SeckillResult process(Reservation r, boolean auto) {
    var user = users.getByUserId(r.userId());
    return tx.fulfill(r, user == null ? null : user.getLevel(), auto, queueSeconds);
  }
}
