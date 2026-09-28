#!/usr/bin/env bash
# Starts exactly two disposable local MySQL instances. Never uses localhost:3306.
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for executable in mysqld mysql mysqladmin python3 mvn; do
  command -v "$executable" >/dev/null || { echo "Missing executable: $executable" >&2; exit 1; }
done
test_root="$(mktemp -d "${SECKILL_TEST_TMPDIR:-/tmp}/seckill-v2-mysql.XXXXXXXX")"
mysql_pids=()
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ "${SECKILL_KEEP_TEST_SERVERS:-0}" == 1 ]]; then
    printf 'Test servers retained; artifacts: %s; PIDs: %s\n' "$test_root" "${mysql_pids[*]}" >&2
    exit "$status"
  fi
  for pid in "${mysql_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${mysql_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [[ "$status" != 0 || "${SECKILL_KEEP_TEST_DATA:-0}" == 1 ]]; then
    echo "Isolated test artifacts retained: $test_root" >&2
  else
    rm -rf -- "$test_root"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
read -r port0 port1 < <(python3 - <<'PY'
import socket
with socket.socket() as a, socket.socket() as b:
    a.bind(('127.0.0.1', 0)); b.bind(('127.0.0.1', 0))
    print(a.getsockname()[1], b.getsockname()[1])
PY
)
ports=("$port0" "$port1")
mysql_user="$(id -un)"
for shard in 0 1; do
  data_dir="$test_root/mysql$shard"
  mkdir "$data_dir"
  mysqld --no-defaults --initialize-insecure --user="$mysql_user" \
    --datadir="$data_dir" --log-error="$test_root/init$shard.log"
  mysqld --no-defaults --user="$mysql_user" --datadir="$data_dir" \
    --bind-address=127.0.0.1 --port="${ports[$shard]}" \
    --socket="$test_root/mysql$shard.sock" --pid-file="$test_root/mysql$shard.pid" \
    --log-error="$test_root/mysql$shard.log" --mysqlx=OFF \
    --innodb-buffer-pool-size=64M --max-connections=40 --skip-log-bin \
    --default-time-zone=+00:00 &
  mysql_pids+=("$!")
  ready=0
  for attempt in $(seq 1 120); do
    if mysqladmin --no-defaults --protocol=SOCKET --socket="$test_root/mysql$shard.sock" -uroot ping >/dev/null 2>&1; then
      ready=1; break
    fi
    kill -0 "${mysql_pids[$shard]}" 2>/dev/null || { cat "$test_root/mysql$shard.log" >&2; exit 1; }
    sleep 0.25
  done
  [[ "$ready" == 1 ]] || { echo "MySQL $shard failed to start" >&2; exit 1; }
done
python3 "$project_root/scripts/seckill_v2_migrate.py" init \
  --target-schemas hmdp_v2_test_0 hmdp_v2_test_1 --output-dir "$test_root/schema"
for shard in 0 1; do
  mysql --no-defaults --protocol=SOCKET --socket="$test_root/mysql$shard.sock" -uroot < "$test_root/schema/init_$shard.sql"
done
for shard in 0 1; do
  python3 "$project_root/tests/mysql/test_order_identity_upgrade.py" --socket "$test_root/mysql$shard.sock"
done
echo "Two isolated MySQL instances ready on ports $port0 and $port1."
printf 'MYSQL0_URL=jdbc:mysql://127.0.0.1:%s/hmdp_v2_test_0\nMYSQL1_URL=jdbc:mysql://127.0.0.1:%s/hmdp_v2_test_1\nMYSQL_PIDS=%s\n' \
  "$port0" "$port1" "${mysql_pids[*]}" > "$test_root/connections.txt"
if [[ "${SECKILL_SCHEMA_ONLY:-0}" == 1 ]]; then
  echo "Schema-only verification passed on both independent MySQL instances."
  exit 0
fi
cd "$project_root"
mvn -pl hmdp-core-service -am test \
  -Dtest=SeckillShardingIT -Dsurefire.failIfNoSpecifiedTests=false \
  "-Dseckill.test.mysql0=jdbc:mysql://127.0.0.1:$port0/hmdp_v2_test_0?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
  "-Dseckill.test.mysql1=jdbc:mysql://127.0.0.1:$port1/hmdp_v2_test_1?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
  -Dseckill.test.mysqlUser=root -Dseckill.test.mysqlPassword= "$@"
