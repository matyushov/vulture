#!/usr/bin/env bash
# Restore verification for the vulture ClickHouse history, on scratch containers.
#
#   deploy/clickhouse/verify-restore.sh
#
# 1. starts a scratch ClickHouse (no published ports) with the same server config as deploy/docker-compose.yml,
# 2. applies the schema from crates/vulture-server/migrations and fills every table with synthetic rows,
# 3. takes a full BACKUP to a local disk (a Docker volume), adds more rows and takes an incremental BACKUP,
# 4. destroys the scratch server together with its data,
# 5. starts a fresh server, RESTOREs from the incremental backup and
# 6. compares row counts and an order-independent checksum of every table with the values recorded in step 3.
#
# Exit code 0 means the restore reproduced the data exactly. Only local Docker is used: no cloud service, no host
# port. Everything it creates (two containers, one volume) is removed at the end. This verifies the local-disk
# variant; the S3 variant in BACKUP.md is documented but not exercised here.
#
# Environment: CLICKHOUSE_IMAGE (default clickhouse/clickhouse-server:25.8), KEEP=1 to keep the containers and the
# volume for inspection (remove them yourself afterwards).
set -euo pipefail

IMAGE="${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:25.8}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS="$HERE/../../crates/vulture-server/migrations"
RUN="vulture-restore-verify-$$"
VOLUME="$RUN-backups"
USER_NAME=verify
PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
WORK="$(mktemp -d)"

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "KEEP=1: leaving containers $RUN-a $RUN-b and volume $VOLUME in place"
  else
    docker rm -f -v "$RUN-a" "$RUN-b" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# Statements given with -q must not read stdin (an INSERT ... VALUES would wait for more data), so only chi() passes -i.
ch() { # ch <container> [clickhouse-client args...]
  local c="$1"; shift
  docker exec "$c" clickhouse-client --user "$USER_NAME" --password "$PASSWORD" "$@"
}
chi() { # chi <container> [clickhouse-client args...]: statements come from stdin
  local c="$1"; shift
  docker exec -i "$c" clickhouse-client --user "$USER_NAME" --password "$PASSWORD" "$@"
}

start() { # start <container>
  docker run -d --name "$1" \
    -e CLICKHOUSE_USER="$USER_NAME" -e CLICKHOUSE_PASSWORD="$PASSWORD" \
    -v "$VOLUME:/backups" \
    -v "$HERE/config.d/vulture.xml:/etc/clickhouse-server/config.d/vulture.xml:ro" \
    --ulimit nofile=262144:262144 \
    "$IMAGE" >/dev/null
  local i
  for i in $(seq 1 60); do
    if ch "$1" -q 'SELECT 1' >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "ClickHouse in $1 did not become ready" >&2
  docker logs "$1" 2>&1 | tail -20 >&2
  return 1
}

# Row count and an order-independent checksum of all columns, one line per table.
fingerprint() { # fingerprint <container>
  local t
  for t in liquidations open_interest gaps signals schema_migrations; do
    printf '%s ' "$t"
    ch "$1" -q "SELECT count(), sum(cityHash64(*)) FROM vulture.$t FORMAT TSV"
  done
}

echo "== scratch server A ($IMAGE)"
docker volume create "$VOLUME" >/dev/null
start "$RUN-a"
ch "$RUN-a" -q 'CREATE DATABASE vulture'

echo "== schema from $MIGRATIONS"
for f in "$MIGRATIONS"/*.sql; do
  chi "$RUN-a" --database vulture --multiquery <"$f"
done
ch "$RUN-a" --database vulture -q "CREATE TABLE schema_migrations (version UInt32, name String, checksum String, applied_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = MergeTree ORDER BY version"
ch "$RUN-a" --database vulture -q "INSERT INTO schema_migrations (version, name, checksum) VALUES (1, 'create_history_tables', 'scratch')"

echo "== synthetic data (two months, nullable values, maps)"
# 2026-09-01 00:00 UTC is 1788220800000 ms; one row a minute, so 50000 rows cross a month boundary.
chi "$RUN-a" --database vulture --multiquery <<'SQL'
INSERT INTO liquidations SELECT 'binance', concat('SYM', toString(number % 50), '-USDT-PERP'), concat('SYM', toString(number % 50), 'USDT'),
  fromUnixTimestamp64Milli(toInt64(1788220800000 + number * 60000)), fromUnixTimestamp64Milli(toInt64(1788220800000 + number * 60000 + 80)),
  CAST(number % 3 AS Int8), 100 + number / 7, 0.5 + (number % 11), (100 + number / 7) * (0.5 + (number % 11)), concat(toString(1788220800000 + number * 60000), '-', toString(number % 5))
FROM numbers(50000);
INSERT INTO open_interest SELECT 'binance', concat('SYM', toString(number % 500), '-USDT-PERP'), concat('SYM', toString(number % 500), 'USDT'),
  fromUnixTimestamp64Milli(toInt64(1788220800000 + intDiv(number, 500) * 30000)), fromUnixTimestamp64Milli(toInt64(1788220800000 + intDiv(number, 500) * 30000 + 120)),
  1000 + number / 3, if(number % 2 = 0, 5.0e7 + number, NULL), if(number % 3 = 0, 1.5 + number % 100, NULL), concat(toString(1788220800000 + number), '-0')
FROM numbers(300000);
INSERT INTO gaps SELECT 'binance', 'open_interest', fromUnixTimestamp64Milli(toInt64(1788220800000 + number * 3600000)), fromUnixTimestamp64Milli(toInt64(1788220800000 + number * 3600000 + 90000)), concat('g-', toString(number))
FROM numbers(1000);
INSERT INTO signals SELECT CAST(number % 4 AS Int8), 'binance', concat('SYM', toString(number % 20), '-USDT-PERP'), 300, 12.5 + number % 7, 1000.5 + number, 10, fromUnixTimestamp64Milli(toInt64(1788220800000 + number * 600000)),
  map('symbols', toString(number % 9), 'peak', toString(number * 10)), concat('s-', toString(number))
FROM numbers(500);
SQL

echo "== full backup"
ch "$RUN-a" -q "BACKUP DATABASE vulture TO Disk('backups', 'verify_full') FORMAT TSV"

echo "== more data, then incremental backup"
chi "$RUN-a" --database vulture --multiquery <<'SQL'
INSERT INTO open_interest SELECT 'binance', concat('SYM', toString(number % 500), '-USDT-PERP'), concat('SYM', toString(number % 500), 'USDT'),
  fromUnixTimestamp64Milli(toInt64(1788220800000 + 86400000 * 40 + intDiv(number, 500) * 30000)), fromUnixTimestamp64Milli(toInt64(1788220800000 + 86400000 * 40 + intDiv(number, 500) * 30000 + 100)),
  2000 + number / 3, NULL, NULL, concat('late-', toString(number))
FROM numbers(20000);
INSERT INTO gaps VALUES ('binance', 'liquidations', fromUnixTimestamp64Milli(1790000000000), fromUnixTimestamp64Milli(1790000060000), 'g-late');
SQL
ch "$RUN-a" -q "BACKUP DATABASE vulture TO Disk('backups', 'verify_incr') SETTINGS base_backup = Disk('backups', 'verify_full') FORMAT TSV"
fingerprint "$RUN-a" >"$WORK/before.txt"
echo "recorded:"; sed 's/^/  /' "$WORK/before.txt"

echo "== destroy server A and its data"
docker rm -f -v "$RUN-a" >/dev/null

echo "== fresh server B, restore from the incremental backup"
start "$RUN-b"
if ch "$RUN-b" -q 'SELECT count() FROM vulture.gaps' >/dev/null 2>&1; then
  echo "FAIL: server B already has data before the restore" >&2; exit 1
fi
ch "$RUN-b" -q "RESTORE DATABASE vulture FROM Disk('backups', 'verify_incr') FORMAT TSV"
fingerprint "$RUN-b" >"$WORK/after.txt"
echo "restored:"; sed 's/^/  /' "$WORK/after.txt"

if diff -u "$WORK/before.txt" "$WORK/after.txt"; then
  echo "PASS: row counts and checksums of all tables match after the restore"
else
  echo "FAIL: the restored data differs from the backed up data" >&2
  exit 1
fi
