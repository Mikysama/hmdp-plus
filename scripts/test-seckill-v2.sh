#!/usr/bin/env bash
set -euo pipefail
# Does not boot the application's development configuration.
cd "$(dirname "$0")/.."
mvn -Dmaven.repo.local=.m2/repository -pl hmdp-core-service -am \
  '-Dtest=InventoryTest,Seckill*Test' -Dsurefire.failIfNoSpecifiedTests=false test
python3 -m unittest discover -s tests/redis_v2 -p 'test_*.py'
python3 -m unittest discover -s sql/v2 -p 'test_*.py'
(cd hmdp-vue3 && npm test && npm run build)
