FROM rust:1.87-bookworm AS builder

# librocksdb-sys compiles RocksDB from source and uses bindgen, which needs
# libclang. cmake + g++ are required for the C++ build itself.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        cmake \
        g++ \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Copy manifests first for layer caching; .dockerignore drops macOS ._*/.DS_Store
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Build the server, gateway AND the `vectordb` CLI (the CLI is needed inside
# the runtime image for `docker compose` healthchecks like
# `vectordb --endpoint http://localhost:6334 health`).
RUN cargo build --release \
    -p vectordb-server \
    -p vectordb-gateway \
    -p vectordb-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/vectordb-server  /usr/local/bin/
COPY --from=builder /app/target/release/vectordb-gateway /usr/local/bin/
COPY --from=builder /app/target/release/vectordb         /usr/local/bin/
VOLUME ["/data"]
# 6333 router gRPC, 6334/6335 shard gRPC, 7334 raft, 8080 gateway HTTP
EXPOSE 6333 6334 6335 7334 8080
ENV VECTORDB_DATA_DIR=/data
# No ENTRYPOINT — the image ships `vectordb-server`, `vectordb-gateway`
# and the `vectordb` CLI. Callers (docker-compose, `docker run`) pick the
# binary via CMD, e.g.
#   command: ["vectordb-server", "--config", "/etc/vectordb/node.toml"]
#   command: ["vectordb-gateway"]
# `VECTORDB_DATA_DIR=/data` above covers the server's --data-dir flag.
CMD ["vectordb-server"]
