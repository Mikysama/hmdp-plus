#!/usr/bin/env bash
set -euo pipefail

# Set MYSQL_HOST, MYSQL_PORT, MYSQL_USER and MYSQL_PWD in the environment.
mysql_host="${MYSQL_HOST:-127.0.0.1}"
mysql_port="${MYSQL_PORT:-3306}"
mysql_user="${MYSQL_USER:-root}"
mysql_cmd=(mysql --host="$mysql_host" --port="$mysql_port" --user="$mysql_user" --batch --skip-column-names)

for schema in hmdp_0 hmdp_1; do
  for suffix in 0 1; do
    table="tb_voucher_order_${suffix}"
    duplicate_groups=$("${mysql_cmd[@]}" "$schema" -e \
      "SELECT COUNT(*) FROM (SELECT 1 FROM ${table} WHERE status = 1 GROUP BY user_id, voucher_id HAVING COUNT(*) > 1) d")
    if [[ "$duplicate_groups" != "0" ]]; then
      echo "${schema}.${table}: ${duplicate_groups} duplicate active-order groups; clean them before migration" >&2
      exit 1
    fi
    index_definition=$("${mysql_cmd[@]}" information_schema -e \
      "SELECT CONCAT(MIN(non_unique), ':', GROUP_CONCAT(column_name ORDER BY seq_in_index)) FROM statistics WHERE table_schema = '${schema}' AND table_name = '${table}' AND index_name = 'uk_voucher_order_user_active_voucher'")
    if [[ "$index_definition" == "0:user_id,active_voucher_id" ]]; then
      echo "${schema}.${table}: unique index already present"
      continue
    fi
    if [[ "$index_definition" != "NULL" ]]; then
      echo "${schema}.${table}: an index with the expected name has a different definition" >&2
      exit 1
    fi
    column_count=$("${mysql_cmd[@]}" information_schema -e \
      "SELECT COUNT(*) FROM columns WHERE table_schema = '${schema}' AND table_name = '${table}' AND column_name = 'active_voucher_id'")
    if [[ "$column_count" == "0" ]]; then
      "${mysql_cmd[@]}" "$schema" -e \
        "ALTER TABLE ${table} ADD COLUMN active_voucher_id BIGINT UNSIGNED GENERATED ALWAYS AS (CASE WHEN status = 1 THEN voucher_id ELSE NULL END) STORED, ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id)"
    else
      "${mysql_cmd[@]}" "$schema" -e \
        "ALTER TABLE ${table} ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id)"
    fi
    echo "${schema}.${table}: unique index installed"
  done
done
