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

### The data directory and the access token

The server keeps its state in one host directory, bind-mounted at `/var/lib/vulture` (`VULTURE_DATA_DIR` in `.env`,
default `./data` next to the compose file). Create it before the first start, otherwise Docker creates it owned by
root and the server cannot write to it:

```sh
cd deploy
mkdir -p data
# Default: the container user (65532) owns it. Reading data/token then needs sudo.
sudo chown 65532:65532 data && sudo chmod 700 data
# Or run the container as yourself, then no sudo is needed to read or edit the token. In .env:
#   VULTURE_UID=<output of `id -u`>   VULTURE_GID=<output of `id -g`>
# and no chown at all (a directory you created is already yours).
```

| file in the data directory | what it is | mode |
|---|---|---|
| `token` | the access token, one line, clear text | `0600` |
| `state.json` | symbol policies set at runtime (versioned) | `0600` |
| `audit.log`, `audit.log.1` | JSON lines of every policy change, rotated at 4 MiB | `0600` |

**First start.** If `data/token` does not exist the server creates it (32 random bytes, base64url, 43 characters,
mode `0600`). If it exists it is used as it is and **never overwritten**. A file that is unreadable or not a valid
token stops the start with a message that names the file and never shows its content. Without a terminal (Docker,
systemd) the token is not printed anywhere: the log has one line saying where the file is. Only a run on an
interactive terminal (`token create`, or the server started by hand) prints a one-time banner.

**Read it.** On the host: `cat data/token`. Or through the container, which needs no host permissions:

```sh
docker compose exec vulture vulture-server token show --config /etc/vulture/config.toml
```

**Change it.** Any of these; a running server notices within `server.token_check_interval_ms` (1 s by default),
switches to the new token and ends the streams opened with the old one (clients get `UNAUTHENTICATED`):

```sh
# 1. Edit the file (the permissions the editor leaves are not important for the server, but keep 0600):
$EDITOR data/token
# 2. Set a token you chose, read from stdin (it never appears on a command line):
printf '%s' 'your-own-token-at-least-32-chars-A-Za-z0-9_-' | docker compose exec -T vulture vulture-server token set --config /etc/vulture/config.toml
# 3. Generate a new random one and print it:
docker compose exec vulture vulture-server token rotate --config /etc/vulture/config.toml
```

An edited token is trimmed (surrounding whitespace and the trailing newline are ignored) and must have at least 32
characters from `A-Z a-z 0-9 - _`. Anything else is rejected: the previous token stays active, the server logs
`token file edit rejected` (without the content) and `vulture_token_file_invalid_edits_total` counts it. A
successful change is logged as `token changed from file` and counted in `vulture_token_file_changes_total`. If the
file is deleted while the server runs, authentication keeps working with the last good token (a warning is
logged) and the file is not recreated until the next start.

**Permissions and backups.** The token is a bearer credential stored in clear text: treat `data/` like a TLS
private key. Keep it `0600` in a `0700` directory; `vulture-server config check` warns when the token file is
readable by group or others. Back up the whole directory (the token with `state.json`) with the same care as any
secret, and rotate the token if a backup leaks. The image never contains a token.

### Migrating from the named volume (`vulture_vulture-data`)

Earlier versions kept these files in the Docker volume `vulture_vulture-data` (the project name `vulture` plus the
volume name). To move them to a host directory, run on the server, from `deploy/`. Add the same `-f` arguments to
every `docker compose` command if you use the TLS override. Nothing is deleted until step 7.

1. Remember the token to compare later and see what is there:
   `docker compose exec -T vulture vulture-server token show --config /etc/vulture/config.toml | sha256sum`
   and `docker run --rm -v vulture_vulture-data:/v:ro alpine:3 ls -la /v`.
2. Pull the new compose files and `.env.example`; add `VULTURE_DATA_DIR`, `VULTURE_UID` and `VULTURE_GID` to your
   `.env` (see below for the values). Check: `docker compose config -q`.
3. Stop the stack, keeping the volumes (never `-v`): `docker compose down`.
4. Create the target and copy with ownership and modes preserved (the helper runs as root inside the container):
   ```sh
   mkdir -p data
   docker run --rm -v vulture_vulture-data:/from:ro -v "$PWD/data":/to alpine:3 cp -a /from/. /to/
   ```
5. Decide who owns `data/` and say so in `.env`. Either keep the container user: leave `VULTURE_UID=65532` and
   `VULTURE_GID=65532` (the copied files already belong to it; read the token with sudo). Or use your own user:
   `sudo chown -R "$(id -u):$(id -g)" data`, then set `VULTURE_UID` and `VULTURE_GID` to `id -u` and `id -g`. In
   the second case the TLS files must be readable by the same ids: `sudo chown <uid>:<gid> <tls dir>/*.pem` (or
   renew once with `VULTURE_TLS_OWNER=<uid>:<gid>` set for the certbot hook; see `certbot/README.md`).
6. Start and verify: `docker compose up -d`, `docker compose ps` (healthy), then `sha256sum < data/token` equals the
   hash from step 1 (use `sudo` if the directory is not yours), `ls -la data` shows `token`, `state.json` and
   `audit.log`, and a client with the old token still connects. The log shows no `token file created` line.
7. Only after you have checked that everything works, remove the old volume: `docker volume rm vulture_vulture-data`.
   To go back before this step, restore the previous compose files and `.env` and `docker compose up -d`; the old
   volume is untouched.

### Try it

```sh
export VULTURE_TOKEN=$(cat data/token)
cargo run -p vulture-server --example stream_client -- --endpoint http://127.0.0.1:50051 --resume-from 0 --max-events 10
```

### Upgrade

```sh
git pull && docker compose build && docker compose up -d
```

Only the image changes. The data directory (`data/` with the token) and the `redis-data` volume with the stream are
kept, so the token and the 24-hour hot window survive. Open streams end with `UNAVAILABLE` during the restart and clients resume
from their last event id.

## TLS

Without `[tls]` the gRPC connection is plaintext: the compose setup keeps it on the host's loopback (use an SSH
tunnel or a TLS-terminating proxy to reach it from elsewhere). To serve TLS directly:

1. Get a certificate (`certbot/README.md`), so that `fullchain.pem` and `privkey.pem` are in a directory readable by
   `VULTURE_UID:VULTURE_GID` (65532 by default, the container user; with your own ids set `VULTURE_TLS_OWNER` for the hook), for example `/etc/vulture/tls` (the certbot hook maintains it).
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

## Container logs

Every service in the compose files logs with the `json-file` driver limited to 5 files of 50 MB, so Docker's logs
cannot fill the disk. The access token is never logged. `docker compose logs -f vulture` follows the server.

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
`/var/lib/vulture/state.json` (`data/state.json` on the host, mode `0600`, versioned) in the data directory and overrides the configuration default
of that exchange from then on; every change is also appended to `/var/lib/vulture/audit.log` (JSON lines, rotated
at 4 MiB to `audit.log.1`). Both files live in the data directory next to the token: they survive upgrades
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
