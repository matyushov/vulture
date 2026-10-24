# Certificates with certbot (DNS-01)

vulture-server serves TLS from two PEM files (`tls.cert` = the full chain, `tls.key`) and reloads them without
dropping open streams. This directory shows how to get and renew a certificate with
[certbot](https://certbot.eff.org/) and how to make the server pick it up. Nothing here names a DNS provider or a
real domain: replace the placeholders.

DNS-01 proves control of a domain through a TXT record, so the server does not need to be reachable from the
internet on port 80 and the certificate can be issued for a name that only resolves on an internal network.

## 1. Get the certificate

Pick the certbot DNS plugin for your DNS provider (`certbot-dns-<provider>`; the list is in the certbot
documentation), install it, and put the provider credentials in a file readable by root only (never in this
repository).

```sh
# vulture.example.com is a placeholder; <provider> is your plugin, for example the one for your DNS host.
sudo certbot certonly \
  --authenticator dns-<provider> \
  --dns-<provider>-credentials /etc/letsencrypt/<provider>-credentials.ini \
  -d vulture.example.com \
  --deploy-hook /etc/letsencrypt/renewal-hooks/deploy/vulture-deploy-hook.sh
```

Install the hook first:

```sh
sudo install -m 0755 vulture-deploy-hook.sh /etc/letsencrypt/renewal-hooks/deploy/vulture-deploy-hook.sh
```

Certbot renews automatically (a systemd timer or cron job installed with the package); the deploy hook runs only
after a successful renewal.

## 2. What the hook does

certbot keeps its files root-only under `/etc/letsencrypt/live/<name>/` as symlinks into `archive/`. The server
runs as an unprivileged user, so the hook **copies** `fullchain.pem` and `privkey.pem` into the TLS directory the
server reads (`/etc/vulture/tls` by default), owned by the server's user with mode `0600`, replacing each file
atomically. Then it asks the server to reload:

| deployment | reload command (done by the hook) |
|---|---|
| docker compose | `docker compose [-f <file>] [--project-directory <dir>] kill -s HUP vulture` |
| systemd | `systemctl kill --signal=SIGHUP vulture.service` |

Settings come from environment variables, see the header of `vulture-deploy-hook.sh`. For compose, set
`VULTURE_TLS_DIR` in `deploy/.env` to the same directory so that it is mounted into the container at
`/etc/vulture/tls`, and set `VULTURE_COMPOSE_FILE` (the compose file, for example `/srv/vulture/deploy/docker-compose.yml`;
several files separated by spaces, for example the TLS override) and `VULTURE_COMPOSE_DIR` (the directory with
`.env`, passed as `--project-directory`) so that the hook can find the project from certbot's environment. The
service is addressed by its compose name `vulture` (`VULTURE_SERVICE` to change it), not by a container name. For systemd use `VULTURE_RELOAD=systemd`. Pass them to certbot with `--deploy-hook
'VULTURE_RELOAD=systemd /etc/letsencrypt/renewal-hooks/deploy/vulture-deploy-hook.sh'` or export them in a
wrapper.

## 3. Check

```sh
vulture-server tls check --config /etc/vulture/config.toml    # files parse, key matches, expiry
curl -s http://127.0.0.1:9100/metrics | grep vulture_tls      # expiry seconds, reload counters
sudo certbot renew --dry-run                                   # runs the whole chain against the staging CA
```

If a renewal produces files the server cannot use (parse error, key mismatch, expired certificate), the server
keeps the old certificate, logs the error and increments `vulture_tls_reloads_total{result="error"}`; the alert
rules in `deploy/prometheus/alerts.yml` fire on that and on a certificate that is close to expiry (warning below 14
days).
