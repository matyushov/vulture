#!/bin/sh
# certbot deploy hook: copy the renewed certificate where vulture-server reads it and tell the server to reload.
#
# certbot runs this after every successful renewal and sets RENEWED_LINEAGE (for example
# /etc/letsencrypt/live/vulture.example.com). The server reloads on SIGHUP and, as a fallback, notices the changed
# files by itself (tls.reload_interval_secs); open streams are not dropped, new connections get the new certificate.
#
# Configuration through the environment (defaults in brackets):
#   VULTURE_TLS_DIR    directory the server reads cert and key from       [/etc/vulture/tls]
#   VULTURE_TLS_OWNER  owner of the copied files, user:group or uid:gid   [65532:65532, the container user]
#   VULTURE_RELOAD     "docker" (compose) or "systemd"                     [docker]
#   VULTURE_CONTAINER  container name for docker mode                      [vulture-vulture-1]
#   VULTURE_UNIT       unit name for systemd mode                          [vulture.service]
#
# Install: copy to /etc/letsencrypt/renewal-hooks/deploy/ (or pass --deploy-hook) and make it executable.
set -eu

: "${RENEWED_LINEAGE:?RENEWED_LINEAGE is not set; run this script from certbot (--deploy-hook)}"
dir="${VULTURE_TLS_DIR:-/etc/vulture/tls}"
owner="${VULTURE_TLS_OWNER:-65532:65532}"
mode="${VULTURE_RELOAD:-docker}"

umask 077
mkdir -p "$dir"
# Write next to the destination, then rename: the server never sees a half-written file. The key goes first, so
# a poll between the two renames sees a (briefly mismatched) pair at worst, which the server rejects and retries
# when the certificate file changes a moment later.
for name in privkey fullchain; do
    install -m 0600 -o "${owner%%:*}" -g "${owner##*:}" "$RENEWED_LINEAGE/$name.pem" "$dir/.$name.pem.new"
    mv -f "$dir/.$name.pem.new" "$dir/$name.pem"
done

case "$mode" in
    docker)
        docker kill --signal HUP "${VULTURE_CONTAINER:-vulture-vulture-1}" ;;
    systemd)
        systemctl kill --signal=SIGHUP "${VULTURE_UNIT:-vulture.service}" ;;
    *)
        echo "unknown VULTURE_RELOAD=$mode (use docker or systemd)" >&2
        exit 1 ;;
esac
echo "vulture: certificate installed in $dir and reload requested"
