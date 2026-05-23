FROM rust:1.87-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p vectordb-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/vectordb-server /usr/local/bin/
VOLUME ["/data"]
EXPOSE 6334
ENV VECTORDB_DATA_DIR=/data
ENTRYPOINT ["vectordb-server", "--data-dir", "/data"]
