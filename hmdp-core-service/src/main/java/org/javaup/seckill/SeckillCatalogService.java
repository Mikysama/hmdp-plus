package org.javaup.seckill;

import static org.javaup.constant.Constant.DELAY_VOUCHER_REMINDER;
import static org.javaup.seckill.SeckillStore.*;

import com.alibaba.fastjson.JSON;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.javaup.context.DelayQueueContext;
import org.javaup.core.SpringUtil;
import org.javaup.delay.message.DelayedVoucherReminderMessage;
import org.javaup.dto.*;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Catalog writes share the voucher inventory lock and never perform network IO in a transaction.
 */
@Service
public class SeckillCatalogService {
  private final SeckillStore store;
  private final SeckillTransactions tx;
  private final SnowflakeIdGenerator ids;
  private final DelayQueueContext delays;
  private final long reminderAheadSeconds;

  public SeckillCatalogService(
      SeckillStore store,
      SeckillTransactions tx,
      SnowflakeIdGenerator ids,
      DelayQueueContext delays,
      @Value("${seckill.reminder.ahead.seconds:120}") long reminderAheadSeconds) {
    this.store = store;
    this.tx = tx;
    this.ids = ids;
    this.delays = delays;
    this.reminderAheadSeconds = Math.max(0, reminderAheadSeconds);
  }

  public long addOrdinary(VoucherDto dto) {
    validateBase(
        dto.getShopId(),
        dto.getTitle(),
        dto.getSubTitle(),
        dto.getRules(),
        dto.getPayValue(),
        dto.getActualValue(),
        dto.getType(),
        dto.getStatus(),
        0);
    long id = ids.nextId();
    return tx.in(
        () -> {
          insertBase(
              id,
              dto.getShopId(),
              dto.getTitle(),
              dto.getSubTitle(),
              dto.getRules(),
              dto.getPayValue(),
              dto.getActualValue(),
              0,
              dto.getStatus());
          return id;
        });
  }

  public long addSeckill(SeckillVoucherDto dto) {
    validateBase(
        dto.getShopId(),
        dto.getTitle(),
        dto.getSubTitle(),
        dto.getRules(),
        dto.getPayValue(),
        dto.getActualValue(),
        dto.getType(),
        dto.getStatus(),
        1);
    validateRules(dto.getBeginTime(), dto.getEndTime(), dto.getAllowedLevels(), dto.getMinLevel());
    if (dto.getStock() == null || dto.getStock() < 0) throw invalid("INVALID_STOCK");
    long id = ids.nextId(), stockId = ids.nextId();
    return tx.in(
        () -> {
          insertBase(
              id,
              dto.getShopId(),
              dto.getTitle(),
              dto.getSubTitle(),
              dto.getRules(),
              dto.getPayValue(),
              dto.getActualValue(),
              1,
              dto.getStatus());
          require(
              store.jdbc.update(
                  "INSERT INTO"
                      + " tb_seckill_voucher(id,voucher_id,init_stock,stock,reserved_stock,sold_stock,version,rule_version,admission_epoch,admission_state,projection_seq,begin_time,end_time,allowed_levels,min_level)"
                      + " VALUES(?,?,?,?,0,0,0,0,0,'PAUSED',0,?,?,?,?)",
                  stockId,
                  id,
                  dto.getStock(),
                  dto.getStock(),
                  dto.getBeginTime(),
                  dto.getEndTime(),
                  normalizeLevels(dto.getAllowedLevels()),
                  dto.getMinLevel()));
          enqueueReminder(id, dto.getBeginTime(), 0);
          return id;
        });
  }

  private void insertBase(
      long id,
      long shop,
      String title,
      String subtitle,
      String rules,
      long pay,
      long actual,
      int type,
      int status) {
    require(
        store.jdbc.update(
            "INSERT INTO"
                + " tb_voucher(id,shop_id,title,sub_title,rules,pay_value,actual_value,type,status)"
                + " VALUES(?,?,?,?,?,?,?,?,?)",
            id,
            shop,
            title,
            subtitle,
            rules,
            pay,
            actual,
            type,
            status));
  }

  public Map<String, Object> get(long voucherId) {
    var stock = store.voucher(voucherId, false);
    var base = store.one("SELECT * FROM tb_voucher WHERE id=?", voucherId);
    if (base == null) throw new SeckillFailure("VOUCHER_NOT_FOUND", 404);
    var result = new LinkedHashMap<String, Object>();
    result.put("id", String.valueOf(voucherId));
    result.put("voucherId", String.valueOf(voucherId));
    String[][] baseFields = {
      {"shop_id", "shopId"},
      {"title", "title"},
      {"sub_title", "subTitle"},
      {"rules", "rules"},
      {"pay_value", "payValue"},
      {"actual_value", "actualValue"},
      {"type", "type"},
      {"status", "status"}
    };
    for (var field : baseFields)
      result.put(field[1], field[0].equals("shop_id") ? str(base, field[0]) : base.get(field[0]));
    String[][] stockFields = {
      {"init_stock", "initStock"},
      {"stock", "stock"},
      {"reserved_stock", "reservedStock"},
      {"sold_stock", "soldStock"},
      {"version", "version"},
      {"rule_version", "ruleVersion"},
      {"admission_state", "admissionState"},
      {"allowed_levels", "allowedLevels"},
      {"min_level", "minLevel"},
      {"begin_time", "beginTime"},
      {"end_time", "endTime"}
    };
    for (var field : stockFields)
      result.put(
          field[1], field[0].endsWith("_time") ? time(stock, field[0]) : stock.get(field[0]));
    result.put("cacheFormatVersion", 2);
    return result;
  }

  public void update(UpdateSeckillVoucherDto dto) {
    if (dto.getVoucherId() == null
        || dto.getVoucherId() <= 0
        || dto.getExpectedVersion() == null
        || dto.getExpectedVersion() < 0) throw invalid("INVALID_VERSION");
    if (dto.getAllowedCities() != null && !dto.getAllowedCities().isBlank())
      throw invalid("CITY_RULE_UNSUPPORTED");
    tx.in(
        () -> {
          long id = dto.getVoucherId();
          var current = store.voucher(id, true);
          if (number(current, "version") != dto.getExpectedVersion())
            throw new SeckillFailure("VERSION_CONFLICT", 409);
          tx.open(current);
          var base = store.one("SELECT * FROM tb_voucher WHERE id=?", id);
          if (base == null) throw new SeckillFailure("VOUCHER_NOT_FOUND", 404);
          String title = value(dto.getTitle(), str(base, "title")),
              subtitle = value(dto.getSubTitle(), str(base, "sub_title")),
              rules = value(dto.getRules(), str(base, "rules"));
          long pay = value(dto.getPayValue(), number(base, "pay_value")),
              actual = value(dto.getActualValue(), number(base, "actual_value"));
          int status = value(dto.getStatus(), (int) number(base, "status"));
          validateBase(
              number(base, "shop_id"),
              title,
              subtitle,
              rules,
              pay,
              actual,
              value(dto.getType(), 1),
              status,
              1);
          LocalDateTime begin = value(dto.getBeginTime(), time(current, "begin_time")),
              end = value(dto.getEndTime(), time(current, "end_time"));
          String levels = value(dto.getAllowedLevels(), str(current, "allowed_levels"));
          Integer minimum =
              value(
                  dto.getMinLevel(),
                  current.get("min_level") == null ? null : (int) number(current, "min_level"));
          validateRules(begin, end, levels, minimum);
          require(
              store.jdbc.update(
                  "UPDATE tb_voucher SET"
                      + " title=?,sub_title=?,rules=?,pay_value=?,actual_value=?,status=?,update_time=CURRENT_TIMESTAMP"
                      + " WHERE id=?",
                  title,
                  subtitle,
                  rules,
                  pay,
                  actual,
                  status,
                  id));
          require(
              store.jdbc.update(
                  "UPDATE tb_seckill_voucher SET"
                      + " begin_time=?,end_time=?,allowed_levels=?,min_level=?,rule_version=rule_version+1,version=version+1,update_time=CURRENT_TIMESTAMP"
                      + " WHERE voucher_id=?",
                  begin,
                  end,
                  normalizeLevels(levels),
                  minimum,
                  id));
          tx.pauseForRebuild(id);
          if (!begin.equals(time(current, "begin_time")))
            enqueueReminder(id, begin, number(current, "rule_version") + 1);
          return null;
        });
  }

  public Map<String, Object> adjust(UpdateSeckillVoucherStockDto dto) {
    if (dto.getVoucherId() == null
        || dto.getVoucherId() <= 0
        || dto.getInitStock() == null
        || dto.getInitStock() < 0
        || dto.getExpectedVersion() == null
        || dto.getExpectedVersion() < 0
        || dto.getAdjustmentId() == null
        || !dto.getAdjustmentId().matches("[A-Za-z0-9_-]{1,64}"))
      throw invalid("INVALID_ADJUSTMENT");
    return tx.adjust(
        dto.getVoucherId(), dto.getAdjustmentId(), dto.getExpectedVersion(), dto.getInitStock());
  }

  private void enqueueReminder(long voucherId, LocalDateTime begin, long ruleVersion) {
    LocalDateTime notifyAt = begin.minusSeconds(reminderAheadSeconds);
    if (!notifyAt.isAfter(store.now(voucherId))) return;
    tx.event(
        voucherId,
        0,
        "REMINDER",
        0,
        null,
        "reminder:" + voucherId + ":" + ruleVersion,
        Map.of("beginTime", begin.toString(), "notifyAt", notifyAt.toString()));
    require(
        store.jdbc.update(
            "UPDATE tb_seckill_outbox SET next_attempt_at=? WHERE voucher_id=? AND event_id=?",
            notifyAt,
            voucherId,
            "reminder:" + voucherId + ":" + ruleVersion));
  }

  public void scheduleReminder(DelayVoucherReminderDto dto) {
    if (dto.getVoucherId() == null
        || dto.getVoucherId() <= 0
        || dto.getDelaySeconds() == null
        || dto.getDelaySeconds() < 0) throw invalid("INVALID_REMINDER");
    tx.in(
        () -> {
          var row = store.voucher(dto.getVoucherId(), true);
          String eventId = "reminder-manual:" + UUID.randomUUID();
          LocalDateTime notifyAt = store.now(dto.getVoucherId()).plusSeconds(dto.getDelaySeconds());
          tx.event(
              dto.getVoucherId(),
              0,
              "REMINDER",
              number(row, "admission_epoch"),
              null,
              eventId,
              Map.of(
                  "beginTime",
                  time(row, "begin_time").toString(),
                  "notifyAt",
                  notifyAt.toString()));
          require(
              store.jdbc.update(
                  "UPDATE tb_seckill_outbox SET next_attempt_at=? WHERE voucher_id=? AND"
                      + " event_id=?",
                  notifyAt,
                  dto.getVoucherId(),
                  eventId));
          return null;
        });
  }

  /** Called by the Outbox worker only after the catalog transaction committed. */
  public void deliverReminder(long voucherId, Map<String, Object> payload) {
    LocalDateTime begin = LocalDateTime.parse(String.valueOf(payload.get("beginTime")));
    var current = store.voucher(voucherId, false);
    if (!begin.equals(time(current, "begin_time")) || !store.now(voucherId).isBefore(begin)) return;
    LocalDateTime notifyAt = LocalDateTime.parse(String.valueOf(payload.get("notifyAt")));
    long delay = Math.max(0, Duration.between(store.now(voucherId), notifyAt).getSeconds());
    String topic = SpringUtil.getPrefixDistinctionName() + "-" + DELAY_VOUCHER_REMINDER;
    delays.sendMessage(
        topic,
        JSON.toJSONString(new DelayedVoucherReminderMessage(voucherId, begin)),
        delay,
        TimeUnit.SECONDS);
  }

  public List<Long> reminderSubscribers(long voucherId, long after, int limit) {
    if (limit < 1 || limit > 100) throw invalid("INVALID_PAGE_SIZE");
    return store.jdbc.queryForList(
        "SELECT user_id FROM tb_seckill_subscription WHERE voucher_id=? AND status='WAITING' AND"
            + " user_id>? ORDER BY user_id LIMIT ?",
        Long.class,
        voucherId,
        after,
        limit);
  }

  /** Durable inbox insertion; it does not claim an external push was delivered. */
  public boolean recordReminder(long voucherId, long userId, LocalDateTime begin, Integer level) {
    return tx.in(
        () -> {
          var voucher = store.voucher(voucherId, true);
          var base = store.one("SELECT status FROM tb_voucher WHERE id=?", voucherId);
          if (base == null
              || number(base, "status") != 1
              || !begin.equals(time(voucher, "begin_time"))
              || !store.now(voucherId).isBefore(begin)
              || !SeckillTransactions.eligible(voucher, level)) return false;
          var subscription =
              store.one(
                  "SELECT status FROM tb_seckill_subscription WHERE voucher_id=? AND user_id=?",
                  voucherId,
                  userId);
          if (subscription == null || !"WAITING".equals(str(subscription, "status"))) return false;
          String eventId = "opening:" + voucherId + ":" + begin + ":" + userId;
          if (store.one(
                  "SELECT event_id FROM tb_seckill_notification WHERE voucher_id=? AND event_id=?",
                  voucherId,
                  eventId)
              != null) return false;
          require(
              store.jdbc.update(
                  "INSERT INTO tb_seckill_notification(event_id,voucher_id,user_id,payload)"
                      + " VALUES(?,?,?,?)",
                  eventId,
                  voucherId,
                  userId,
                  JSON.toJSONString(
                      Map.of(
                          "type",
                          "OPENING_REMINDER",
                          "beginTime",
                          begin.toString(),
                          "delivery",
                          "INBOX"))));
          return true;
        });
  }

  private static <T> T value(T candidate, T fallback) {
    return candidate == null ? fallback : candidate;
  }

  private static SeckillFailure invalid(String code) {
    return new SeckillFailure(code, 400);
  }

  private static void validateBase(
      Long shop,
      String title,
      String subtitle,
      String rules,
      Long pay,
      Long actual,
      Integer type,
      Integer status,
      int expectedType) {
    if (shop == null
        || shop <= 0
        || title == null
        || title.isBlank()
        || title.length() > 255
        || subtitle == null
        || subtitle.isBlank()
        || subtitle.length() > 255
        || (rules != null && rules.length() > 1024)) throw invalid("INVALID_VOUCHER_TEXT");
    if (pay == null || actual == null || pay < 0 || actual <= 0 || pay > actual)
      throw invalid("INVALID_VOUCHER_AMOUNT");
    if (type == null || type != expectedType || status == null || status < 1 || status > 3)
      throw invalid("INVALID_VOUCHER_STATE");
  }

  private static void validateRules(
      LocalDateTime begin, LocalDateTime end, String levels, Integer minimum) {
    if (begin == null || end == null || !begin.isBefore(end))
      throw invalid("INVALID_ACTIVITY_TIME");
    if (minimum != null && (minimum < 0 || minimum > 10)) throw invalid("INVALID_LEVEL_RULE");
    normalizeLevels(levels);
  }

  private static String normalizeLevels(String levels) {
    if (levels == null || levels.isBlank()) return null;
    var parsed = new TreeSet<Integer>();
    try {
      for (String token : levels.split(",", -1)) {
        int level = Integer.parseInt(token.trim());
        if (level < 0 || level > 10) throw invalid("INVALID_LEVEL_RULE");
        parsed.add(level);
      }
    } catch (NumberFormatException e) {
      throw invalid("INVALID_LEVEL_RULE");
    }
    return String.join(",", parsed.stream().map(String::valueOf).toList());
  }
}
