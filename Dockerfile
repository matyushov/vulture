# syntax=docker/dockerfile:1
#
# vulture-server image. Multi-stage: build with the Rust toolchain, run on a small distroless base as a
# non-root user. Nothing secret is baked in: the configuration is mounted read-only at /etc/vulture/config.toml,
# the access token lives in /var/lib/vulture (a host directory bind-mounted by the compose file) and is created only
# when it does not exist yet.
#
#   docker build -t vulture-server .
#
# RUST_VERSION must equal `channel` in rust-toolchain.toml (CI checks it); bump both together.
ARG RUST_VERSION=1.99.1

FROM rust:${RUST_VERSION}-bookworm AS builder
ARG RUST_VERSION
# The image already has this exact toolchain; stop rustup from fetching "stable" per rust-toolchain.toml.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
COPY fixtures ./fixtures
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p vulture-server \
    && install -D target/release/vulture-server /out/usr/local/bin/vulture-server \
    && install -d -m 0700 /out/var/lib/vulture

# Debian 12 userland without a shell or package manager; uid/gid 65532 ("nonroot"). TLS roots for outgoing
# connections are bundled in the binary (webpki-roots), so no CA package is needed.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /out/usr/local/bin/vulture-server /usr/local/bin/vulture-server
# An empty data directory owned by the runtime user: a fresh named volume inherits owner and mode from it. The
# server never overwrites an existing <data_dir>/token, so upgrading the image keeps the token.
COPY --from=builder --chown=65532:65532 /out/var/lib/vulture /var/lib/vulture
VOLUME ["/var/lib/vulture"]
USER 65532:65532
# gRPC and metrics. Publish them deliberately (see deploy/README.md); the metrics port is not authenticated.
EXPOSE 50051 9100
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/vulture-server", "healthcheck", "--config", "/etc/vulture/config.toml"]
ENTRYPOINT ["/usr/local/bin/vulture-server"]
CMD ["run", "--config", "/etc/vulture/config.toml"]
