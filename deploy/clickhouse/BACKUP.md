# Backing up and restoring the ClickHouse history

The history of liquidations and open interest cannot be recovered after the fact: the exchanges do not serve it
back. **Backups are mandatory.** Redis only holds the last 24 hours, so everything older than that exists only in
ClickHouse (and in your backups). Everything here is generic: endpoints, bucket names and keys are placeholders, and
keys come from the environment or secret files, never from files in the repository.

| File | What |
|---|---|
| `config.d/vulture.xml` | server configuration used by compose and by the verification script (declares the `backups` disk) |
| `backup.sh` | `full`, `incr`, `list`, `restore`, `prune` against the compose ClickHouse, local disk |
| `verify-restore.sh` | restore verification on scratch containers (create, back up, destroy, restore, compare) |
| `config.d/backup-s3.xml.example` | named collection for `BACKUP ... TO S3(...)`, keys from the environment |
| `clickhouse-backup.example.yml` | optional configuration of the third-party tool `clickhouse-backup` |

## What to back up

1. **The ClickHouse database** (`vulture`): the history. This is the part that matters.
2. **The data directory** (`data/` next to the compose file, or `VULTURE_DATA_DIR`; `/var/lib/vulture` in the container): `token` (the owner's access token), `state.json` (the symbol
   policies set at runtime) and `audit.log` (their history). Losing the token only means issuing a new one; losing
   `state.json` resets the tracked symbols to the configuration default. These files are secrets or owner decisions:
   back them up encrypted and keep them apart from the ClickHouse backups if those go to a third party.
3. **The configuration**: the `.env` file (passwords), `config.docker.toml`, and any TLS material you cannot reissue.
   The certificates themselves are reissued by certbot.
4. **Redis is not worth backing up.** It holds a 24-hour window plus the flusher watermark. After losing Redis the
   server continues; at most the unflushed tail (up to one flush interval, 10 minutes by default) is gone.

## Local disk (tested)

The compose service mounts a separate volume, `clickhouse-backups`, at `/backups` and declares a ClickHouse disk named
`backups` on it (`config.d/vulture.xml`). On a single machine this protects against a damaged database or a wrong
`DROP`, **not** against losing the machine: copy the volume contents off the host regularly (rsync, a snapshot, `restic`).

```sh
cd deploy
./clickhouse/backup.sh full          # BACKUP DATABASE vulture TO Disk('backups', 'full_<UTC timestamp>')
./clickhouse/backup.sh incr          # only parts added since the newest full backup
./clickhouse/backup.sh list
```

The same in SQL (run with `docker compose exec clickhouse clickhouse-client --user vulture --password "$CLICKHOUSE_PASSWORD"`):

```sql
BACKUP DATABASE vulture TO Disk('backups', 'full_2026-01-01');
BACKUP DATABASE vulture TO Disk('backups', 'incr_2026-01-02')
    SETTINGS base_backup = Disk('backups', 'full_2026-01-01');
```

An incremental backup stores only the data parts that are new since its base. The tables are append-only and
partitioned by month, so a closed month never changes and increments stay small. Restoring an increment needs its base
backup (and the chain between them) on the same disk.

### Restore

Into an **empty** server (a new machine, or after dropping the database): start the compose stack's ClickHouse, copy
the backup directory into the `clickhouse-backups` volume, then

```sh
./clickhouse/backup.sh restore incr_20260102T030000Z
# or: RESTORE DATABASE vulture FROM Disk('backups', 'incr_20260102T030000Z');
```

The restore refuses to overwrite a table that already contains data (`CANNOT_RESTORE_TABLE`); that is intended. Then
start vulture: the migrations see `schema_migrations` restored with the data and do nothing.

The flusher's watermark lives in Redis, not in ClickHouse. Restoring a backup that is **older** than the watermark
leaves a hole: the entries between the backup and the watermark are not re-sent, and Redis no longer holds them if
they are more than 24 hours old. Only when the hole is inside the last 24 hours can you refill it: delete the
`<stream_key>:flusher:watermark` key to start from the oldest entry in Redis; entries that the restored data already
contains would then be inserted a second time (the deduplication tokens of that old range are no longer remembered), so
restrict such a replay to a restore you understand, and remove duplicates by `event_id` afterwards.

### Restore verification (run this after every change of the procedure, and from time to time)

```sh
deploy/clickhouse/verify-restore.sh
```

It starts a scratch ClickHouse (no published ports, same server config as compose), applies the schema from
`crates/vulture-server/migrations`, fills all four tables with synthetic rows (two months, `NULL` values, maps), takes a
full backup, adds rows, takes an incremental backup, **destroys the container with its data**, starts a fresh server,
restores from the incremental backup and compares row counts and an order-independent checksum
(`sum(cityHash64(*))`) of every table with the values recorded before the destruction. Exit code 0 and the line
`PASS` mean the restore reproduced the data exactly. Everything it creates (two containers, one volume) is removed at
the end (`KEEP=1` keeps them). Last run: 2026-10-24, ClickHouse 25.8, PASS (50 000 liquidations, 320 000 open
interest rows, 1 001 gaps, 500 signals).

A backup you have never restored is not a backup: also try restoring a real backup on a spare machine now and then.

## S3-compatible object storage (documented, NOT tested)

The S3 variant was **not exercised**: no cloud service was contacted while this was written. Only the syntax of the
configuration was checked (the named collection loads, the key variables are substituted from the environment).
Verify it against your provider with a small database first, then with `verify-restore.sh` adapted to your target.

1. Copy `config.d/backup-s3.xml.example` to `config.d/backup-s3.xml`, replace `ENDPOINT` and `BUCKET`, and mount it
   next to `vulture.xml` (add a volume line to the `clickhouse` service). Give the container the keys through the
   environment (`BACKUP_S3_KEY_ID`, `BACKUP_S3_SECRET`) from your `.env` or a secret store; never put them into the
   XML. Note that ClickHouse writes the expanded configuration into its data directory
   (`preprocessed_configs`), so the data volume must stay private.
2. Back up and restore with the named collection:

```sql
BACKUP DATABASE vulture TO S3(vulture_backup_s3, 'daily/2026-01-01');
BACKUP DATABASE vulture TO S3(vulture_backup_s3, 'daily/2026-01-02')
    SETTINGS base_backup = S3(vulture_backup_s3, 'daily/2026-01-01');
RESTORE DATABASE vulture FROM S3(vulture_backup_s3, 'daily/2026-01-02');
```

   Without a named collection the form is `S3('https://ENDPOINT/BUCKET/vulture/name', '<access key id>', '<secret>')`;
   avoid it, the keys then sit in the query text and in `system.query_log`.
3. Use a bucket that only this service can write and that the server cannot delete from (versioning or object lock),
   so that a compromised server cannot erase its own backups.

### Optional: `clickhouse-backup`

[`clickhouse-backup`](https://github.com/Altinity/clickhouse-backup) is a separate tool that does the same job with
its own retention handling. `clickhouse-backup.example.yml` is a starting point (placeholders only, also untested
here). The built-in `BACKUP` command is enough for vulture and has no extra moving part.

## Retention suggestion

The data is append-only and small (about 16 GiB a year, see the main README), so keep it simple:

- **Daily** an incremental backup (base: the newest full), **weekly** a new full backup.
- Keep the newest **4 weekly fulls with their increments** (about a month of daily restore points) and, in object
  storage, one **monthly full for 12 months** (an S3 lifecycle rule does it). `backup.sh prune 4` keeps the newest four
  fulls on the local disk and removes everything older.
- Closed months are immutable: a monthly full of a finished month stays valid forever and can be kept as an archive.
- Cron example (UTC, on the host; replace the path):

```cron
15 3 * * 1   /opt/vulture/deploy/clickhouse/backup.sh full  && /opt/vulture/deploy/clickhouse/backup.sh prune 4
15 3 * * 2-7 /opt/vulture/deploy/clickhouse/backup.sh incr
```

  Alert when the newest backup is older than 36 hours (a check on the backup volume or the object store; vulture does
  not export it). Local backups take disk space on the same machine: the free-space alerts count them.
