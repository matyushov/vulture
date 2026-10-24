# Deploying vulture-server

Generic templates and instructions. Nothing here is specific to one deployment: hostnames, domains and paths are
placeholders (`vulture.example.com`, `/etc/vulture`, `/var/lib/vulture`), and secrets live only in `.env` files that
git ignores (`*.example` templates are committed).

| Path | What |
|---|---|
| `../Dockerfile` | multi-stage image of `vulture-server` (distroless, non-root) |
| `docker-compose.yml`, `.env.example`, `config.docker.toml` | vulture + Redis (+ optional ClickHouse, profile `clickhouse`) |
| `clickhouse/` | ClickHouse server config, backup and restore scripts, `BACKUP.md` |
| `docker-compose.tls.yml`, `config.docker.tls.toml` | override that turns TLS on (certificate mount, published gRPC port) |
| `certbot/` | DNS-01 certificate with certbot and a deploy hook that reloads the server |
| `systemd/vulture.service` | example unit for running the binary without Docker |
| `prometheus/` | sample scrape configuration and alert rules |

## First start with Docker Compose

```sh
cd deploy
cp .env.example .env              # set REDIS_PASSWORD (letters and digits)
docker compose build
docker compose up -d
docker compose ps                 # both services become healthy
```

By default both ports are published to `127.0.0.1` of the host only: gRPC `50051` (plaintext, see below) and
metrics `9100`. Redis is not published at all; only the vulture container reaches it.

### The access token

On the first start the server creates the token in the `vulture-data` volume (`/var/lib/vulture/token`, mode `0600`)
and prints it once to the container log. Show it again at any time (the image has no shell, so call the binary):

```sh
docker compose exec vulture vulture-server token show --config /etc/vulture/config.toml
```

Treat the token like a password; the volume and its backups are secret too. `token rotate` replaces it (the old
one stops working at once). The image never contains a token and the server never overwrites an existing one, so
upgrades keep it.

### Try it

```sh
export VULTURE_TOKEN=$(docker compose exec -T vulture vulture-server token show --config /etc/vulture/config.toml)
cargo run -p vulture-server --example stream_client -- --endpoint http://127.0.0.1:50051 --resume-from 0 --max-events 10
```

### Upgrade

```sh
git pull && docker compose build && docker compose up -d
```

Only the image changes. The volumes (`vulture-data` with the token, `redis-data` with the stream) are kept, so the
token and the 24-hour hot window survive. Open streams end with `UNAVAILABLE` during the restart and clients resume
from their last event id.

## TLS

Without `[tls]` the gRPC connection is plaintext: the compose setup keeps it on the host's loopback (use an SSH
tunnel or a TLS-terminating proxy to reach it from elsewhere). To serve TLS directly:

1. Get a certificate (`certbot/README.md`), so that `fullchain.pem` and `privkey.pem` are in a directory readable by
   uid `65532` (the container user), for example `/etc/vulture/tls` (the certbot hook maintains it).
2. In `.env` set `VULTURE_TLS_DIR=/etc/vulture/tls`.
3. Start with the TLS override: `docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d`. It mounts
   `config.docker.tls.toml` (`[tls]` enabled, `allow_plaintext = false`; uncomment `client_ca` there for mTLS) and
   the certificate directory, and publishes the gRPC port on `VULTURE_GRPC_BIND` (all interfaces by default in
   the override; set `127.0.0.1` in `.env` to keep it local). Open the port in your firewall deliberately.
   Use the same two `-f` arguments for every later compose command (`ps`, `logs`, `kill`, `up`).
   Validate without starting: `docker compose -f docker-compose.yml -f docker-compose.tls.yml --env-file
   .env.example config -q`. The override needs Docker Compose 2.24 or newer.

Check the files without starting anything: `docker compose run --rm --no-deps vulture tls check --config
/etc/vulture/config.toml` (the entrypoint is the binary).

### Renewals reload without a restart

The server reloads the certificate and key on `SIGHUP`, and also when the files' modification time or size
changes (`tls.reload_interval_secs`, default 30 s; `0` turns polling off). New connections get the new certificate,
streams that are already open keep running. If the new files are unusable (parse error, key mismatch, expired
certificate), the old certificate stays in use, the error is logged without key material and
`vulture_tls_reloads_total{result="error"}` counts it. The expiry is logged at start and on every reload (a warning
below 14 days) and exported as `vulture_tls_certificate_expiry_seconds`. `client_ca` is read at start only.

Manual reload: `docker compose kill -s HUP vulture` (compose; add `-f docker-compose.yml -f
docker-compose.tls.yml` and `--project-directory <dir>` when you run it from another directory, as the certbot hook
does through `VULTURE_COMPOSE_FILE` and `VULTURE_COMPOSE_DIR`) or `systemctl reload vulture` (systemd). A `SIGHUP`
is harmless when TLS is off.

## Metrics

```sh
curl -s http://127.0.0.1:9100/metrics | head      # Prometheus text format
curl -s http://127.0.0.1:9100/healthz              # process is up
curl -s http://127.0.0.1:9100/readyz               # Redis reachable and the gRPC listener running (503 otherwise)
```

The endpoint is **not authenticated**. It listens on loopback by default (inside the container on all interfaces,
published to the host's loopback). Expose it only through an internal network or a reverse proxy that adds
authentication. Series are labelled by `exchange`, `type`, `state` and similar low-cardinality labels, never by
symbol or client address. `prometheus/prometheus.example.yml` and `prometheus/alerts.yml` are a starting point. The
container health check runs `vulture-server healthcheck` (a request to `/healthz`).

## Without Docker (systemd)

Install the binary, a config file (copy `../config.example.toml`), an environment file with
`VULTURE__REDIS__URL=redis://:<password>@127.0.0.1:6379` (not in the repository, mode `0640`) and
`systemd/vulture.service`; the header of the unit lists the commands. `systemctl reload vulture` sends `SIGHUP`.
Set `VULTURE_RELOAD=systemd` for the certbot hook.

## Tracked symbols and the state file

The `symbols` list of an `[[exchanges]]` entry is only the **default** policy (empty = every symbol). The owner
changes it at runtime with the `Control` gRPC service (`ListSymbolPolicies`, `SetSymbolPolicy`; try
`cargo run -p vulture-server --example control_client -- --endpoint ... list`). What the owner sets is stored in
`/var/lib/vulture/state.json` (mode `0600`, versioned) in the data volume and overrides the configuration default
of that exchange from then on; every change is also appended to `/var/lib/vulture/audit.log` (JSON lines, rotated
at 4 MiB to `audit.log.1`). Both files live in the `vulture-data` volume next to the token: they survive upgrades
and are never overwritten by the image, and they belong in the backups. A damaged or unknown-version
`state.json` stops the server from starting with an explanatory message (it does not fall back to the
configuration, which could re-enable symbols you removed): fix or delete the file deliberately.

## Redis

The compose file starts Redis with `appendonly yes`, `appendfsync everysec`, `maxmemory 3gb` and
`maxmemory-policy noeviction`, and a password from `.env`. 24 hours of events take roughly 260 MiB at the expected
rate, so 3 GB is generous; a full Redis refuses writes and the server buffers events in memory (and records a `Gap`
if even that overflows) instead of losing history silently.

## ClickHouse history (optional)

Without it nothing changes: no flusher runs and Redis keeps 24 hours, trimmed by time. To keep the history forever:

1. In `.env` set `CLICKHOUSE_PASSWORD` (letters and digits; quote it if it could read as a number) and
   ```
   COMPOSE_PROFILES=clickhouse
   VULTURE_CLICKHOUSE_ENABLED=true
   ```
2. `docker compose up -d`. The `clickhouse` service (image tag pinned to `25.8`, upgrade it deliberately) has a named
   volume for data, a separate volume for backups, a health check, no published port (vulture reaches it at
   `http://clickhouse:8123` over the compose network) and a minimal server configuration
   (`clickhouse/config.d/vulture.xml`: unused ports closed, system logs bounded, memory capped at 40% of RAM). For a
   look inside: `docker compose exec clickhouse clickhouse-client --user vulture --password "$CLICKHOUSE_PASSWORD"`.
   If you must publish a port, bind it to loopback (`127.0.0.1:8123:8123`).
3. The server applies the schema at start (`vulture-server migrate --config /etc/vulture/config.toml` does it by hand,
   idempotently and without ever dropping anything) and the flusher moves the Redis stream to ClickHouse every 10 minutes.

Enabling it later is safe: the first flush starts from the oldest entry in Redis. Turning it off again leaves the data
in place; Redis goes back to time trimming (the watermark is no longer consulted).

**Disk.** Plan for about 16 GiB a year at 560 symbols and 30 s snapshots (measurements in the main README). Merges
temporarily need extra room, up to the size of the partitions being merged, so alert on free space early: the alert rules
`VultureClickHouseDiskLow` (under 25% or 20 GiB free) and `VultureClickHouseDiskCritical` use the gauges that vulture
samples from `system.disks` every minute. Local backups live on the same machine and count against the same disk.

**Backups are mandatory**: see [clickhouse/BACKUP.md](clickhouse/BACKUP.md) (`backup.sh`, the restore verification
script, the S3 variant, retention, and what else to save: `state.json`, `audit.log`, the token file and the `.env`).

**If ClickHouse is down for long.** The watermark stays, Redis is not trimmed by time past it, and the flusher catches
up after the recovery. The one limit is `redis.maxlen_safeguard` (5 million entries, about 66 hours at the expected 21 events/s; watch
`vulture_flusher_backlog_maxlen_ratio`): beyond it Redis drops the oldest unflushed entries to protect its memory. The alerts `VultureClickHouseDown` (5 minutes) and `VultureFlusherBacklogNearLoss` (70%) fire first.
