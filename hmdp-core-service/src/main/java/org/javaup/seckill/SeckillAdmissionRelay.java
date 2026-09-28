package org.javaup.seckill;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.javaup.seckill.redis.RedisAdmissionGateway;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** At-least-once admission delivery. XRANGE retains ownership in Redis until Kafka ACK. */
@Slf4j
@Component
@ConditionalOnProperty(name = "seckill.v2.workers-enabled", havingValue = "true", matchIfMissing = true)
public class SeckillAdmissionRelay {
  private final SeckillStore store;
  private final RedisAdmissionGateway redis;
  private final SeckillAdmissionPublisher publisher;
  private final MeterRegistry metrics;
  // Cursors are only scan optimizations. A restarted worker begins at the oldest retained entry.
  private record Scan(String after, String through) {}
  private final Map<Long, Scan> streamCursors = new HashMap<>();
  private long voucherCursor;

  public SeckillAdmissionRelay(SeckillStore store, RedisAdmissionGateway redis,
      SeckillAdmissionPublisher publisher, MeterRegistry metrics) {
    this.store = store;
    this.redis = redis;
    this.publisher = publisher;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${seckill.v2.stream-relay-ms:500}", scheduler = "seckillStreamScheduler")
  public synchronized void relay() {
    var vouchers = store.vouchers(voucherCursor, 100);
    if (vouchers.isEmpty()) { voucherCursor = 0; return; }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    for (long voucher : vouchers) {
      voucherCursor = voucher;
      try { relayVoucher(voucher, deadline); }
      catch (RuntimeException e) {
        metrics.counter("seckill_v2_stream_retry", "stage", "scan").increment();
        log.warn("Admission stream scan failed voucher={}", voucher, e);
      }
      // A single synchronous Kafka send may exceed the budget; never start another after that.
      if (System.nanoTime() >= deadline) return;
    }
  }

  private void relayVoucher(long voucher, long deadline) {
    Scan scan = streamCursors.get(voucher);
    if (scan == null) {
      String tail = redis.admissionOutboxTail(voucher);
      if (tail == null) return;
      scan = new Scan(null, tail);
    }
    // Freeze a high-water mark for this pass: continuous arrivals cannot starve failed entries.
    var rows = redis.admissionOutbox(voucher, scan.after(), scan.through(), 100);
    if (rows.isEmpty()) { streamCursors.remove(voucher); return; }
    for (var row : rows) {
      String id = row.getId().getValue();
      streamCursors.put(voucher, new Scan(id, scan.through()));
      try {
        var reservation = redis.decodeAdmission(voucher, row.getValue());
        String auto = String.valueOf(row.getValue().get("autoIssue"));
        if (!"true".equals(auto) && !"false".equals(auto))
          throw new IllegalStateException("INVALID_AUTO_ISSUE");
        // Do not infer a database failure from a missing/rebuilt Redis hold. The consumer
        // decides terminal/expired/stale-epoch outcomes under the database voucher lock.
        publisher.publish(reservation, Boolean.parseBoolean(auto));
        redis.acknowledgeAdmission(voucher, id);
        metrics.counter("seckill_v2_stream_delivered").increment();
      } catch (RuntimeException e) {
        // Includes ACK followed by XDEL failure: retain and resend with the same identity.
        metrics.counter("seckill_v2_stream_retry", "stage", "delivery").increment();
        log.warn("Admission stream entry retained voucher={} streamId={} reason={}", voucher, id, e.toString());
      }
      if (id.equals(scan.through())) { streamCursors.remove(voucher); return; }
      if (System.nanoTime() >= deadline) return;
    }
    if (rows.size() < 100) streamCursors.remove(voucher);
  }
}
