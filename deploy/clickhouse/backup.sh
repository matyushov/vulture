#!/usr/bin/env bash
# Backups of the compose ClickHouse to its local backup disk (the clickhouse-backups volume, mounted at /backups).
#
#   deploy/clickhouse/backup.sh full                 # complete copy of the vulture database
#   deploy/clickhouse/backup.sh incr                 # only what changed since the newest full backup
#   deploy/clickhouse/backup.sh list                 # backups on the disk, oldest first
#   deploy/clickhouse/backup.sh restore <name>       # RESTORE into an EMPTY server (see BACKUP.md)
#   deploy/clickhouse/backup.sh prune [fulls]        # keep the newest <fulls> full backups (default 4) and what depends on them
#
# Names sort by time: full_YYYYmmddTHHMMSSZ and incr_YYYYmmddTHHMMSSZ. A local-disk backup does not survive the loss
# of the machine: copy the volume off the host (or use the S3 variant described in BACKUP.md). The script talks to
# the container with `docker compose exec` and the credentials that are already in the container's environment, so
# no password appears on a command line. Run it from anywhere; it uses deploy/docker-compose.yml and deploy/.env.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$HERE/$(basename "${BASH_SOURCE[0]}")"
cd "$HERE/.."
DB="${VULTURE_CH_DATABASE:-vulture}"
compose() { docker compose --profile clickhouse "$@"; }
client() { # client <sql>
  compose exec -T clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$1"' sh "$1"
}
stamp() { date -u +%Y%m%dT%H%M%SZ; }
names() { # backup directories on the disk, oldest first
  compose exec -T clickhouse sh -c 'ls -1 /backups 2>/dev/null' | tr -d '\r' | grep -E '^(full|incr)_[0-9TZ]+$' | sort -t_ -k2,2 || true
}

cmd="${1:-}"
case "$cmd" in
  full)
    name="full_$(stamp)"
    client "BACKUP DATABASE $DB TO Disk('backups', '$name') FORMAT TSV"
    echo "created $name"
    ;;
  incr)
    base="$(names | grep '^full_' | tail -n 1 || true)"
    [ -n "$base" ] || { echo "no full backup yet: run '$0 full' first" >&2; exit 1; }
    name="incr_$(stamp)"
    client "BACKUP DATABASE $DB TO Disk('backups', '$name') SETTINGS base_backup = Disk('backups', '$base') FORMAT TSV"
    echo "created $name (base $base)"
    ;;
  list)
    names
    ;;
  restore)
    name="${2:?usage: $0 restore <name>}"
    echo "restoring $name into an empty server; an existing database $DB makes this fail on purpose"
    client "RESTORE DATABASE $DB FROM Disk('backups', '$name') FORMAT TSV"
    ;;
  prune)
    keep="${2:-4}"
    cutoff="$(names | grep '^full_' | tail -n "$keep" | head -n 1 || true)"
    [ -n "$cutoff" ] || { echo "fewer than $keep full backups: nothing to prune"; exit 0; }
    # Everything older than the oldest kept full (its incrementals are newer, so they stay with it).
    for n in $(names); do
      ts="${n#*_}"; cts="${cutoff#*_}"
      if [[ "$ts" < "$cts" ]]; then
        echo "removing $n"
        compose exec -T clickhouse sh -c 'rm -rf -- "/backups/$1"' sh "$n"
      fi
    done
    ;;
  *)
    sed -n '2,8p' "$SELF" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
