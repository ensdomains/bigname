# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.93.1

FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo

# Third-party dependencies compile from the workspace manifests alone, against
# placeholder sources, so editing first-party code does not recompile them. Every
# workspace member is listed: a new member without a line here fails to resolve.
COPY apps/api/Cargo.toml apps/api/
COPY apps/phase-runner/Cargo.toml apps/phase-runner/
COPY crates/adapters/Cargo.toml crates/adapters/
COPY crates/content-hash/Cargo.toml crates/content-hash/
COPY crates/domain/Cargo.toml crates/domain/
COPY crates/ingest/Cargo.toml crates/ingest/
COPY crates/interpret/Cargo.toml crates/interpret/
COPY crates/lookup/Cargo.toml crates/lookup/
COPY crates/manifests/Cargo.toml crates/manifests/
COPY crates/metrics/Cargo.toml crates/metrics/
COPY crates/project/Cargo.toml crates/project/
COPY crates/storage/Cargo.toml crates/storage/
COPY crates/test-support/Cargo.toml crates/test-support/
COPY tools/benchmark-gate/Cargo.toml tools/benchmark-gate/

RUN set -eu; \
    for member in apps/phase-runner crates/adapters crates/content-hash crates/domain \
        crates/ingest crates/interpret crates/lookup crates/manifests crates/metrics \
        crates/project crates/storage crates/test-support; do \
        mkdir -p "$member/src" && : > "$member/src/lib.rs"; \
    done; \
    for member in apps/api apps/phase-runner tools/benchmark-gate; do \
        mkdir -p "$member/src" && echo 'fn main() {}' > "$member/src/main.rs"; \
    done; \
    for member in apps/phase-runner crates/content-hash; do \
        echo 'fn main() {}' > "$member/build.rs"; \
    done

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release --workspace --bins

COPY apps apps
COPY crates crates
COPY tools tools
COPY migrations migrations
COPY manifests manifests
COPY schema-v2 schema-v2

# Declared after the dependency build so that a new commit does not invalidate it.
ARG BIGNAME_BUILD_SHA=unknown
ENV BIGNAME_BUILD_SHA=${BIGNAME_BUILD_SHA}

# Cargo decides freshness by modification time, and a fresh checkout carries times
# older than the placeholder build above, so the real sources are marked newer here.
# Without this cargo keeps some placeholder crates as fresh, and the build either
# fails against them or ships them.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    find apps crates tools migrations manifests schema-v2 -exec touch {} + \
    && cargo build --locked --release --workspace --bins

FROM ubuntu:24.04 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libstdc++6 tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 bigname \
    && useradd --system --uid 10001 --gid bigname --home-dir /app --create-home bigname

WORKDIR /app

COPY --from=builder /app/target/release/bigname-api /usr/local/bin/bigname-api
COPY --from=builder /app/target/release/phase-runner /usr/local/bin/phase-runner
COPY --from=builder --chown=bigname:bigname /app/manifests /app/manifests
COPY --chmod=0755 docker/entrypoint.sh /usr/local/bin/bigname

ENV BIGNAME_API_BIND_ADDR=0.0.0.0:3000 \
    BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT=/app/manifests/mainnet \
    RUST_LOG=info

EXPOSE 3000

USER bigname

ENTRYPOINT ["tini", "--", "bigname"]
CMD ["api"]
