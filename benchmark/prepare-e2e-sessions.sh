#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?usage: prepare-e2e-sessions.sh RUN_ID SESSION_COUNT [BASE_USER_ID]}"
session_count="${2:?usage: prepare-e2e-sessions.sh RUN_ID SESSION_COUNT [BASE_USER_ID]}"
# Redis Lua 5.1 represents numbers as doubles. Keep generated IDs below 2^53 so
# tostring() remains an exact decimal value that Java can parse as a Long.
base_user_id="${3:-9000000000}"
redis_cli="${REDIS_CLI:-redis-cli}"

if ! [[ "$run_id" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "RUN_ID may contain only letters, digits, underscore and hyphen" >&2
  exit 2
fi
if ! [[ "$session_count" =~ ^[0-9]+$ ]] || (( session_count < 1 )); then
  echo "SESSION_COUNT must be a positive integer" >&2
  exit 2
fi

batch_size=1000
for ((offset = 0; offset < session_count; offset += batch_size)); do
  remaining=$((session_count - offset))
  count=$((remaining < batch_size ? remaining : batch_size))
  "$redis_cli" --raw EVAL \
    "for i=0,tonumber(ARGV[3])-1 do local n=tonumber(ARGV[2])+i; local key='login:token:'..ARGV[1]..':'..n; redis.call('HSET',key,'id',tostring(tonumber(ARGV[4])+n),'nickName','benchmark-user'); redis.call('EXPIRE',key,3600); end; return tonumber(ARGV[3])" \
    0 "$run_id" "$offset" "$count" "$base_user_id" >/dev/null
done

echo "Prepared $session_count Redis login sessions for run '$run_id'."
