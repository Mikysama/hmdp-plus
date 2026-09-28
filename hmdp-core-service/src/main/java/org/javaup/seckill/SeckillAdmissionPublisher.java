package org.javaup.seckill;

import com.alibaba.fastjson.JSON;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.javaup.seckill.redis.RedisAdmissionGateway.Reservation;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** No database dependency: broker acknowledgment is the QUEUED boundary. */
@Component
public class SeckillAdmissionPublisher {
  public static final String TOPIC = "seckill-admission-v4";
  private final KafkaTemplate<String, String> kafka;

  public SeckillAdmissionPublisher(
      @Qualifier("seckillV2Template") KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  public void publish(Reservation r, boolean auto) {
    String payload =
        JSON.toJSONString(
            Map.of(
                "schemaVersion",
                4,
                "eventId",
                "admit:" + r.orderId(),
                "reservation",
                Map.of(
                    "requestId",
                    r.requestId(),
                    "orderId",
                    r.orderId(),
                    "voucherId",
                    String.valueOf(r.voucherId()),
                    "userId",
                    String.valueOf(r.userId()),
                    "epoch",
                    String.valueOf(r.epoch()),
                    "ruleVersion",
                    String.valueOf(r.ruleVersion()),
                    "createdAt",
                    r.createdAt(),
                    "state",
                    r.state()),
                "autoIssue",
                auto));
    try {
      kafka.send(TOPIC, String.valueOf(r.voucherId()), payload).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SeckillFailure("DELIVERY_UNCONFIRMED", 503);
    } catch (Exception e) {
      // A timeout is not proof of non-delivery. Keep ownership for retry/reconciliation.
      throw new SeckillFailure("DELIVERY_UNCONFIRMED", 503);
    }
  }
}
