package org.javaup.seckill.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/** Redis is a recoverable admission projection. No database outcome is inferred here. */
@Component
public class RedisAdmissionGateway {
  // Delivery mode is checked by reserve.lua and sent as the separate Kafka autoIssue field.
  @com.fasterxml.jackson.annotation.JsonIgnoreProperties({"autoIssue"})
  public record Reservation(
      String requestId,
      String orderId,
      long voucherId,
      long userId,
      long epoch,
      long ruleVersion,
      long createdAt,
      String state) {}

  private final StringRedisTemplate redis;
  private final ObjectMapper json;
  private final String prefix;
  private final String buyerStatsPrefix;
  private final int rate;
  private final int concurrency;

  @Value("${seckill.v2.orphan-seconds:30}")
  private long orphanSeconds = 30;

  @Value("${seckill.v2.queue-seconds:60}")
  private long queueSeconds = 60;

  private final DefaultRedisScript<String> issue = script("issue_token");
  private final DefaultRedisScript<String> reserve = script("reserve");
  private final DefaultRedisScript<String> projection = script("project");
  private final DefaultRedisScript<String> lease = script("lease");
  private final DefaultRedisScript<String> stage = script("stage");
  private final DefaultRedisScript<String> audit = script("audit");
  private final DefaultRedisScript<String> auditPage = script("audit_page");
  private final DefaultRedisScript<String> buyerStats = script("buyer_stats");
  private final DefaultRedisScript<String> attemptSlot = script("attempt_slot");

  public RedisAdmissionGateway(
      StringRedisTemplate redis,
      ObjectMapper json,
      @Value("${seckill.v2.prefix:hmdp:v3}") String prefix,
      @Value("${seckill.v2.buyer-stats-prefix:hmdp:v2}") String buyerStatsPrefix,
      @Value("${seckill.v2.admission-rate:100}") int rate,
      @Value("${seckill.v2.admission-concurrency:32}") int concurrency) {
    this.redis = redis;
    this.json = json;
    this.prefix = prefix;
    this.buyerStatsPrefix = buyerStatsPrefix;
    if (rate <= 0 || concurrency <= 0)
      throw new IllegalArgumentException("Positive admission limits required");
    this.rate = rate;
    this.concurrency = concurrency;
  }

  private static DefaultRedisScript<String> script(String name) {
    DefaultRedisScript<String> s = new DefaultRedisScript<>();
    s.setLocation(new ClassPathResource("lua/v2/" + name + ".lua"));
    s.setResultType(String.class);
    return s;
  }

  private String base(long voucherId) {
    return prefix + ":{" + voucherId + "}";
  }

  private List<String> keys(long voucherId, long epoch, long user) {
    String root = base(voucherId), b = root + ":epoch:" + epoch;
    return List.of(
        root + ":active",
        b + ":meta",
        b + ":stock",
        b + ":users",
        b + ":requests",
        b + ":reservations",
        b + ":pending",
        b + ":inflight",
        b + ":bucket",
        root + ":token:" + user);
  }

  private List<String> fenced(long voucherId, List<String> keys) {
    List<String> result = new ArrayList<>(keys);
    result.add(base(voucherId) + ":fence");
    return result;
  }

  /** Every HTTP retry uses a NEW server-generated attempt ID, independent of request identity. */
  public boolean tryEnter(long voucherId, String attemptId) {
    validateAttempt(attemptId);
    return "ENTERED"
        .equals(
            execute(
                attemptSlot,
                List.of(base(voucherId) + ":attempts"),
                "ENTER",
                attemptId,
                concurrency,
                30_000));
  }

  public void leave(long voucherId, String attemptId) {
    validateAttempt(attemptId);
    execute(
        attemptSlot,
        List.of(base(voucherId) + ":attempts"),
        "LEAVE",
        attemptId,
        concurrency,
        30_000);
  }

  private static void validateAttempt(String id) {
    if (id == null || id.isBlank() || id.length() > 128)
      throw new IllegalArgumentException("Invalid attempt ID");
  }

  /** Availability probe has no business-key dependency and performs no writes. */
  public boolean available() {
    try {
      String pong =
          redis.execute(
              (org.springframework.data.redis.core.RedisCallback<String>)
                  connection -> connection.ping());
      return "PONG".equalsIgnoreCase(pong);
    } catch (RuntimeException unavailable) {
      return false;
    }
  }

  /** Bounded audit: BUSY means concurrent projection/rebuild or more than 2000 records. */
  public String auditProjection(long voucherId, long epoch, long sequence, long total) {
    return execute(
        audit,
        fenced(voucherId, keys(voucherId, epoch, 0).subList(0, 8)),
        epoch,
        sequence,
        total,
        2000);
  }

  /** Full optimistic scan for large vouchers. Any concurrent mutation returns BUSY. */
  public String auditProjectionPaged(long voucherId, long epoch, long sequence, long total) {
    List<String> k = fenced(voucherId, keys(voucherId, epoch, 0).subList(0, 8));
    String version = "";
    Set<String> live = new HashSet<>();
    for (String phase : List.of("reservations", "users", "requests")) {
      String cursor = "0";
      do {
        com.fasterxml.jackson.databind.JsonNode page =
            parse(execute(auditPage, k, epoch, sequence, phase, cursor, version));
        if ("BUSY".equals(page.path("status").asText())) return "BUSY";
        version = page.path("version").asText();
        for (com.fasterxml.jackson.databind.JsonNode id : page.path("live")) live.add(id.asText());
        cursor = page.path("cursor").asText();
      } while (!"0".equals(cursor));
    }
    com.fasterxml.jackson.databind.JsonNode last =
        parse(execute(auditPage, k, epoch, sequence, "finish", "0", version));
    if ("BUSY".equals(last.path("status").asText())) return "BUSY";
    if (last.path("stock").asLong() + live.size() != total)
      throw new IllegalStateException("CORRUPT_AUDIT_TOTAL");
    return "MATCH";
  }

  private com.fasterxml.jackson.databind.JsonNode parse(String value) {
    try {
      return json.readTree(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("CORRUPT_AUDIT_RESULT", e);
    }
  }

  /** date is the ORIGINAL purchase date, including cancellation delivered on later days. */
  public String projectBuyerStats(
      long shopId, String date, long userId, String orderId, String eventId, String kind) {
    java.time.LocalDate.parse(date);
    if (orderId == null || eventId == null || kind == null)
      throw new IllegalArgumentException("Missing statistics event identity");
    String root = buyerStatsPrefix + ":buyers:{" + shopId + ":" + date + "}";
    return execute(
        buyerStats,
        List.of(root + ":scores", root + ":events", root + ":orders"),
        eventId,
        kind,
        userId,
        orderId);
  }

  public record Activity(String status, long beginMillis, long endMillis) {}

  public Activity activity(long voucher) {
    long epoch = activeEpoch(voucher);
    var meta = redis.opsForHash().entries(keys(voucher, epoch, 0).get(1));
    return new Activity(
        required(meta, "status"),
        Long.parseLong(required(meta, "begin")),
        Long.parseLong(required(meta, "end")));
  }

  public Reservation findReservation(long voucher, long user, String request) {
    long epoch = activeEpoch(voucher);
    var k = keys(voucher, epoch, user);
    Object id = redis.opsForHash().get(k.get(4), user + ":" + request);
    if (id == null) return null;
    Object raw = redis.opsForHash().get(k.get(5), id.toString());
    if (raw == null) throw new IllegalStateException("CORRUPT_REQUEST");
    var r = decode(raw.toString());
    if (r.voucherId() != voucher
        || r.userId() != user
        || !request.equals(r.requestId())
        || !id.toString().equals(r.orderId()))
      throw new IllegalStateException("RESERVATION_MISMATCH");
    return r;
  }

  /** One-key presence probe, not an existence proof or permission to purchase. */
  public boolean hasAdmission(long voucherId) {
    return Boolean.TRUE.equals(redis.hasKey(base(voucherId) + ":active"));
  }

  public long activeEpoch(long voucherId) {
    String current = redis.opsForValue().get(base(voucherId) + ":active");
    if (current == null) throw new IllegalStateException("ADMISSION_UNINITIALIZED");
    return Long.parseLong(current);
  }

  private String execute(DefaultRedisScript<String> script, List<String> keys, Object... args) {
    String[] strings = Arrays.stream(args).map(String::valueOf).toArray(String[]::new);
    try {
      String result = redis.execute(script, keys, (Object[]) strings);
      if (result == null) throw new IllegalStateException("EMPTY_REDIS_RESULT");
      return result;
    } catch (RuntimeException e) {
      Throwable detail = e;
      while (detail.getCause() != null && detail.getCause() != detail) detail = detail.getCause();
      throw new IllegalStateException("Redis admission: " + detail.getMessage(), e);
    }
  }

  public String issueToken(long voucherId, long userId) {
    return execute(issue, List.of(base(voucherId) + ":token:" + userId), UUID.randomUUID(), 30_000);
  }

  public Reservation reserve(
      long voucherId, long userId, String requestId, String accessToken, String orderId) {
    return reserve(voucherId, userId, requestId, accessToken, orderId, false);
  }

  public Reservation reserve(
      long voucherId, long userId, String requestId, String accessToken, String orderId,
      boolean autoIssue) {
    if (requestId == null
        || requestId.isBlank()
        || requestId.length() > 128
        || accessToken == null
        || orderId == null) throw new IllegalArgumentException("Invalid reservation arguments");
    long epoch = activeEpoch(voucherId);
    var reserveKeys = fenced(voucherId, keys(voucherId, epoch, userId));
    reserveKeys.add(admissionOutboxKey(voucherId));
    return decode(
        execute(
            reserve,
            reserveKeys,
            epoch,
            voucherId,
            userId,
            requestId,
            accessToken,
            orderId,
            rate,
            concurrency,
            30_000,
            autoIssue));
  }

  private String admissionOutboxKey(long voucherId) {
    return base(voucherId) + ":admission-outbox";
  }

  /** Stable across recovery epochs. Only broker-acknowledged entries may be deleted. */
  public List<MapRecord<String, Object, Object>> admissionOutbox(long voucherId, String after, int limit) {
    return admissionOutbox(voucherId, after, null, limit);
  }

  public String admissionOutboxTail(long voucherId) {
    var rows = redis.opsForStream().reverseRange(admissionOutboxKey(voucherId),
        Range.unbounded(), Limit.limit().count(1));
    return rows == null || rows.isEmpty() ? null : rows.get(0).getId().getValue();
  }

  public List<MapRecord<String, Object, Object>> admissionOutbox(
      long voucherId, String after, String through, int limit) {
    if (limit < 1 || limit > 100) throw new IllegalArgumentException("Invalid outbox batch size");
    Range<String> range = Range.of(after == null ? Range.Bound.unbounded() : Range.Bound.exclusive(after),
        through == null ? Range.Bound.unbounded() : Range.Bound.inclusive(through));
    var rows = redis.opsForStream().range(admissionOutboxKey(voucherId), range, Limit.limit().count(limit));
    return rows == null ? List.of() : rows;
  }

  public void acknowledgeAdmission(long voucherId, String streamId) {
    redis.opsForStream().delete(admissionOutboxKey(voucherId), RecordId.of(streamId));
  }

  public Reservation decodeAdmission(long voucherId, Map<Object, Object> fields) {
    Object raw = fields.get("reservation");
    if (raw == null) throw new IllegalStateException("MISSING_ADMISSION_PAYLOAD");
    Reservation r = decode(raw.toString());
    if (r.voucherId() != voucherId || r.userId() <= 0 || r.epoch() < 1
        || r.orderId() == null || !r.orderId().matches("[1-9][0-9]{0,18}")
        || Long.parseLong(r.orderId()) <= 0 || r.requestId() == null
        || !r.requestId().matches("[A-Za-z0-9_-]{1,64}")
        || r.ruleVersion() < 0 || r.createdAt() <= 0 || !"HELD".equals(r.state()))
      throw new IllegalStateException("INVALID_ADMISSION_PAYLOAD");
    return r;
  }

  /** Returns APPLIED, DUPLICATE or STALE; a sequence gap throws and must be retried. */
  public String project(
      long voucherId,
      long epoch,
      long sequence,
      String eventId,
      String kind,
      long userId,
      String orderId,
      long delta) {
    long active = activeEpoch(voucherId);
    if (epoch < active) return "STALE";
    if (epoch > active) throw new IllegalStateException("FUTURE_EPOCH");
    return execute(
        projection,
        keys(voucherId, epoch, userId).subList(0, 8),
        epoch,
        sequence,
        eventId,
        kind,
        userId,
        orderId == null ? "" : orderId,
        delta);
  }

  /** Stage only while the database voucher is frozen. Active namespaces are never overwritten. */
  public void rebuild(
      long voucherId,
      long epoch,
      long stock,
      long ruleVersion,
      String status,
      long beginMillis,
      long endMillis,
      long sequence,
      List<Reservation> bindings) {
    if (stock < 0 || epoch < 1 || beginMillis >= endMillis || sequence < 0)
      throw new IllegalArgumentException("Invalid rebuild snapshot");
    List<String> k = fenced(voucherId, keys(voucherId, epoch, 0).subList(0, 9));
    String worker = UUID.randomUUID().toString();
    // Validate and serialize every row before creating the inactive namespace.
    List<Map<String, Object>> snapshots = new ArrayList<>();
    Set<Long> users = new HashSet<>();
    Set<String> orders = new HashSet<>();
    for (Reservation r : bindings) {
      if (r.voucherId() != voucherId
          || r.orderId() == null
          || !r.orderId().matches("[1-9][0-9]{0,18}")
          || !orders.add(r.orderId())
          || !users.add(r.userId())
          || !("ACCEPTED".equals(r.state()) || "COMMITTED".equals(r.state())))
        throw new IllegalArgumentException("Invalid or duplicate active database binding");
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("requestId", r.requestId());
      row.put("orderId", r.orderId());
      row.put("voucherId", "" + voucherId);
      row.put("userId", "" + r.userId());
      row.put("epoch", "" + epoch);
      row.put("ruleVersion", "" + ruleVersion);
      row.put("createdAt", r.createdAt());
      row.put("state", r.state());
      snapshots.add(row);
    }
    execute(
        stage,
        k,
        epoch,
        "BEGIN",
        worker,
        stock,
        ruleVersion,
        status,
        beginMillis,
        endMillis,
        sequence,
        "[]");
    for (int offset = 0; offset < snapshots.size(); offset += 100) {
      execute(
          stage,
          k,
          epoch,
          "APPEND",
          worker,
          stock,
          ruleVersion,
          status,
          beginMillis,
          endMillis,
          sequence,
          encode(snapshots.subList(offset, Math.min(offset + 100, snapshots.size()))));
    }
    execute(
        stage,
        k,
        epoch,
        "FINISH",
        worker,
        stock,
        ruleVersion,
        status,
        beginMillis,
        endMillis,
        sequence,
        "[]");
  }

  public void activate(long voucherId, long epoch) {
    String result =
        execute(
            lease,
            fenced(voucherId, keys(voucherId, epoch, 0).subList(0, 8)),
            epoch,
            "ACTIVATE",
            5000);
    if ("STALE".equals(result)) throw new IllegalStateException("STALE_EPOCH");
  }

  public boolean renew(long voucherId, long epoch) {
    return "RENEWED"
        .equals(
            execute(
                lease,
                fenced(voucherId, keys(voucherId, epoch, 0).subList(0, 8)),
                epoch,
                "RENEW",
                5000));
  }

  public record Inspection(
      long epoch,
      long sequence,
      String state,
      long stock,
      long ruleVersion,
      long reservationCount,
      long leaseUntil) {}

  /** Inspection never creates or repairs missing state. Missing fields trigger recovery. */
  public Inspection inspect(long voucherId) {
    long epoch = activeEpoch(voucherId);
    List<String> k = keys(voucherId, epoch, 0);
    Map<Object, Object> meta = redis.opsForHash().entries(k.get(1));
    String stock = redis.opsForValue().get(k.get(2));
    Long count = redis.opsForHash().size(k.get(5));
    if (stock == null || meta.isEmpty() || count == null || count < 1)
      throw new IllegalStateException("CORRUPT_PROJECTION");
    return new Inspection(
        epoch,
        Long.parseLong(required(meta, "sequence")),
        required(meta, "state"),
        Long.parseLong(stock),
        Long.parseLong(required(meta, "ruleVersion")),
        count - 1,
        Long.parseLong(required(meta, "leaseUntil")));
  }

  private static String required(Map<Object, Object> values, String key) {
    Object value = values.get(key);
    if (value == null) throw new IllegalStateException("CORRUPT_METADATA_" + key);
    return value.toString();
  }

  public void releaseSlot(long voucherId, Reservation reservation) {
    redis
        .opsForZSet()
        .remove(
            keys(voucherId, reservation.epoch(), reservation.userId()).get(7),
            reservation.orderId());
  }

  public void releaseSlot(long voucherId, String orderId) {
    redis.opsForZSet().remove(keys(voucherId, activeEpoch(voucherId), 0).get(7), orderId);
  }

  public List<Reservation> overdue(long voucherId, long now, int limit) {
    if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Invalid scan limit");
    long epoch = activeEpoch(voucherId);
    List<String> k = keys(voucherId, epoch, 0);
    Set<String> ids =
        redis
            .opsForZSet()
            .rangeByScore(
                k.get(6), 1, now - Math.max(orphanSeconds, queueSeconds) * 1000, 0, limit);
    List<Reservation> result = new ArrayList<>();
    if (ids != null)
      for (String id : ids) {
        Object raw = redis.opsForHash().get(k.get(5), id);
        if (raw == null) throw new IllegalStateException("CORRUPT_PENDING_RESERVATION");
        Reservation r = decode(raw.toString());
        if (!id.equals(r.orderId()) || r.voucherId() != voucherId || r.epoch() != epoch)
          throw new IllegalStateException("CORRUPT_PENDING_RESERVATION");
        if ("HELD".equals(r.state())) result.add(r);
      }
    return result;
  }

  private String encode(Object r) {
    try {
      return json.writeValueAsString(r);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Reservation serialization failed", e);
    }
  }

  private Reservation decode(String raw) {
    try {
      return json.readValue(raw, Reservation.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("CORRUPT_RESERVATION", e);
    }
  }
}
