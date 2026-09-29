package org.javaup.seckill;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** All mutating callers supply voucher_id, including CAS/lease updates. */
@Repository
public class SeckillStore {
  public final JdbcTemplate jdbc;

  public SeckillStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Map<String, Object> one(String sql, Object... args) {
    var rows = jdbc.queryForList(sql, args);
    return rows.isEmpty() ? null : rows.get(0);
  }

  public Map<String, Object> voucher(long id, boolean lock) {
    var row =
        one(
            "SELECT * FROM tb_seckill_voucher WHERE voucher_id=?" + (lock ? " FOR UPDATE" : ""),
            id);
    if (row == null) throw new SeckillFailure("VOUCHER_NOT_FOUND", 404);
    return row;
  }

  public Map<String, Object> request(long v, long u, String r) {
    return one(
        "SELECT * FROM tb_seckill_request WHERE voucher_id=? AND user_id=? AND request_id=?",
        v,
        u,
        r);
  }

  public Map<String, Object> orderRequest(long v, long o) {
    return one("SELECT * FROM tb_seckill_request WHERE voucher_id=? AND id=?", v, o);
  }

  public LocalDateTime now(long v) {
    return time(
        one("SELECT CURRENT_TIMESTAMP(3) AS db_now FROM tb_seckill_voucher WHERE voucher_id=?", v),
        "db_now");
  }

  /** All catalog IDs, including ordinary and inactive vouchers, for the existence index. */
  public List<Long> catalogVoucherIds(long after, int count) {
    return jdbc.queryForList(
        "SELECT id FROM tb_voucher WHERE id>? ORDER BY id LIMIT ?", Long.class, after, count);
  }

  public List<Long> vouchers(long after, int count) {
    return jdbc.queryForList(
        "SELECT voucher_id FROM tb_seckill_voucher WHERE voucher_id>? ORDER BY voucher_id LIMIT ?",
        Long.class,
        after,
        count);
  }

  public static long number(Map<String, Object> row, String k) {
    Object v = row.get(k);
    return v == null ? 0 : ((Number) v).longValue();
  }

  public static String str(Map<String, Object> row, String k) {
    Object v = row.get(k);
    return v == null ? null : v.toString();
  }

  public static LocalDateTime time(Map<String, Object> row, String k) {
    Object v = row.get(k);
    if (v == null) return null;
    if (v instanceof java.time.OffsetDateTime t) return t.toLocalDateTime();
    return v instanceof Timestamp t ? t.toLocalDateTime() : (LocalDateTime) v;
  }

  public static Inventory inventory(Map<String, Object> row) {
    return new Inventory(
        (int) number(row, "init_stock"),
        (int) number(row, "stock"),
        (int) number(row, "reserved_stock"),
        (int) number(row, "sold_stock"));
  }

  public void saveInventory(long v, Inventory i) {
    require(
        jdbc.update(
            "UPDATE tb_seckill_voucher SET"
                + " init_stock=?,stock=?,reserved_stock=?,sold_stock=?,version=version+1,update_time=CURRENT_TIMESTAMP"
                + " WHERE voucher_id=?",
            i.total(),
            i.available(),
            i.reserved(),
            i.sold(),
            v));
  }

  public static void require(int count) {
    if (count != 1) throw new IllegalStateException("EXPECTED_ONE_ROW:" + count);
  }

  public static SeckillResult result(Map<String, Object> row) {
    return new SeckillResult(
        str(row, "request_id"),
        str(row, "id"),
        str(row, "status"),
        str(row, "reason_code"),
        time(row, "expires_at"));
  }
}
