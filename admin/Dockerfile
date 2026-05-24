# syntax=docker/dockerfile:1.6
# Multi-stage build for the VectorDB admin panel:
#   1. node  — builds the React frontend  → /frontend/dist
#   2. go    — builds the Echo backend    → /admin
#   3. slim  — combines both into a minimal runtime image
#
# Build context: the repository root (so we can `COPY admin ./admin`).
# Build with:
#   docker build -f admin/Dockerfile -t vectordb-admin .

############################
# Stage 1 — frontend (React)
############################
FROM node:20-bookworm-slim AS frontend
WORKDIR /src

# Install dependencies first for layer caching.
COPY admin/frontend/package.json admin/frontend/package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi

COPY admin/frontend ./
RUN npm run build

############################
# Stage 2 — backend (Go)
############################
FROM golang:1.24-bookworm AS backend
WORKDIR /src/admin/backend

# Bring in go.sum if present so `go mod download` is reproducible. Both files
# describe pure-Go deps (Echo, ledongthuc/pdf, etc.) — no CGO required.
COPY admin/backend/go.mod admin/backend/go.sum* ./
RUN go mod download

COPY admin/backend/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/vectordb-admin .

############################
# Stage 3 — runtime
############################
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=backend  /out/vectordb-admin       /app/vectordb-admin
COPY --from=frontend /src/dist                 /app/frontend/dist

ENV ADMIN_LISTEN=:8090 \
    ADMIN_STATIC=/app/frontend/dist \
    VECTORDB_URL=http://gateway:8080

EXPOSE 8090

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:8090/config.json" >/dev/null || exit 1

ENTRYPOINT ["/app/vectordb-admin"]
