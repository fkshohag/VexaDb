SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Defaults / variables
# ---------------------------------------------------------------------------
COMPOSE       ?= docker compose
GATEWAY_URL   ?= http://127.0.0.1:8080
ROUTER_GRPC   ?= http://127.0.0.1:6333
ADMIN_URL     ?= http://127.0.0.1:8090
CARGO_FLAGS   ?=

# Cluster shape — pass to `make up` to override:
#   make up RF=3 SHARDS=2
RF      ?=
SHARDS  ?=

CLUSTER := ./scripts/cluster.py

# `make all` and `make up` are aliases. Cluster shape is persisted in
# .cluster.state.json so subsequent commands remember RF/SHARDS.
.PHONY: all
all: up ## Build + start the cluster (alias for `up`).

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*##"; printf "Available targets:\n\n"} \
		/^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' \
		$(MAKEFILE_LIST)
	@echo
	@echo "Cluster lifecycle (one source of truth — scripts/cluster.py):"
	@echo "  make up                       # 1 shard, RF=1 (default)"
	@echo "  make up RF=3 SHARDS=2         # 2 shards × 3 replicas = 6 data nodes"
	@echo "  make scale-rf RF=5            # bump replication factor"
	@echo "  make add-shard                # add capacity (auto-rebalance moves data)"
	@echo "  make down                     # stop (keeps volumes)"
	@echo "  make nuke                     # stop and DELETE data volumes"
	@echo

# ---------------------------------------------------------------------------
# Cluster lifecycle (the only docker compose user-facing surface)
# ---------------------------------------------------------------------------

# Build cluster.py args from RF/SHARDS env (omit when unset so cluster.py
# falls back to the persisted state).
CLUSTER_UP_ARGS :=
ifneq ($(RF),)
CLUSTER_UP_ARGS += --rf $(RF)
endif
ifneq ($(SHARDS),)
CLUSTER_UP_ARGS += --shards $(SHARDS)
endif

.PHONY: up
up: ## Generate compose + configs from RF/SHARDS, then start (default RF=1, SHARDS=1).
	$(CLUSTER) up $(CLUSTER_UP_ARGS)

.PHONY: scale-rf
scale-rf: ## Change replication factor in place. Usage: make scale-rf RF=5
	@if [ -z "$(RF)" ]; then echo "ERROR: pass RF=N (e.g. make scale-rf RF=5)"; exit 1; fi
	$(CLUSTER) scale-rf --rf $(RF)

.PHONY: add-shard
add-shard: ## Add one new shard (router auto-rebalance moves ~1/N of data).
	$(CLUSTER) add-shard

.PHONY: down
down: ## Stop the cluster (keeps data volumes).
	$(CLUSTER) down

.PHONY: nuke
nuke: ## Stop and DELETE data volumes (DESTRUCTIVE).
	$(CLUSTER) nuke

.PHONY: ps
ps: ## Show container status.
	$(CLUSTER) ps

.PHONY: logs
logs: ## Tail logs from all services. Pass SERVICE=name for one service.
	$(CLUSTER) logs $(SERVICE)

.PHONY: info
info: ## Print cluster URLs.
	$(CLUSTER) info

.PHONY: render
render: ## Regenerate compose + configs without starting docker.
	$(CLUSTER) render $(CLUSTER_UP_ARGS)

# ---------------------------------------------------------------------------
# Convenience aliases
# ---------------------------------------------------------------------------

.PHONY: restart
restart: down up ## Restart the cluster.

.PHONY: logs-router logs-gateway logs-admin
logs-router:   ## Tail router logs.
	$(CLUSTER) logs router
logs-gateway:  ## Tail gateway logs.
	$(CLUSTER) logs gateway
logs-admin:    ## Tail admin panel logs.
	$(CLUSTER) logs admin

.PHONY: admin-open
admin-open: ## Open the admin panel in your browser.
	@command -v open >/dev/null 2>&1 && open $(ADMIN_URL) || \
		(command -v xdg-open >/dev/null 2>&1 && xdg-open $(ADMIN_URL)) || \
		echo "Open $(ADMIN_URL) manually."

.PHONY: admin-dev
admin-dev: ## Run the admin panel in dev mode (Go + Vite, hot reload).
	cd admin && ./scripts/dev.sh

# ---------------------------------------------------------------------------
# macOS / filesystem hygiene
# ---------------------------------------------------------------------------

.PHONY: clean-macos
clean-macos: ## Strip macOS AppleDouble (._*) and .DS_Store files.
	@find . -name '._*' -type f -delete 2>/dev/null || true
	@find . -name '.DS_Store' -type f -delete 2>/dev/null || true
	@command -v dot_clean >/dev/null 2>&1 && dot_clean -m . || true
	@echo "macOS artifacts cleaned."

# ---------------------------------------------------------------------------
# Smoke test against the running gateway
# ---------------------------------------------------------------------------

SMOKE_COLLECTION ?= smoke-$(shell date +%s)

.PHONY: smoke
smoke: ## End-to-end smoke test against the running gateway.
	@echo "→ waiting for gateway at $(GATEWAY_URL)/health..."
	@for i in $$(seq 1 30); do \
		curl -fsS $(GATEWAY_URL)/health >/dev/null 2>&1 && break; \
		sleep 1; \
		if [ $$i -eq 30 ]; then echo "gateway never became healthy"; exit 1; fi; \
	done
	@echo "→ health"
	@curl -fsS $(GATEWAY_URL)/health && echo
	@echo "→ create collection '$(SMOKE_COLLECTION)'"
	@curl -fsS -X POST $(GATEWAY_URL)/v1/collections \
		-H 'Content-Type: application/json' \
		-d '{"name":"$(SMOKE_COLLECTION)","dimension":3,"payload_indexes":[{"field":"category","kind":"keyword"},{"field":"price","kind":"numeric"}]}' \
		-o /dev/null -w "  HTTP %{http_code}\n"
	@echo "→ upsert 2 points"
	@curl -fsS -X POST $(GATEWAY_URL)/v1/collections/$(SMOKE_COLLECTION)/upsert \
		-H 'Content-Type: application/json' \
		-d '{"points":[{"id":"a","values":[1,0,0],"payload":{"category":"books","price":20}},{"id":"b","values":[0.95,0.1,0],"payload":{"category":"movies","price":15}}]}' \
		&& echo
	@echo "→ filtered search (category=books, price<=50)"
	@curl -fsS -X POST $(GATEWAY_URL)/v1/collections/$(SMOKE_COLLECTION)/search \
		-H 'Content-Type: application/json' \
		-d '{"vector":[1,0,0],"top_k":5,"filter":{"must":[{"key":"category","match":{"value":"books"}},{"key":"price","range":{"lte":50}}]}}' \
		&& echo
	@echo "→ cleanup: delete '$(SMOKE_COLLECTION)'"
	@curl -fsS -X DELETE $(GATEWAY_URL)/v1/collections/$(SMOKE_COLLECTION) \
		-o /dev/null -w "  HTTP %{http_code}\n" || true

# ---------------------------------------------------------------------------
# Cargo / Rust workspace
# ---------------------------------------------------------------------------

.PHONY: check
check: ## cargo check on the full workspace.
	cargo check --workspace $(CARGO_FLAGS)

.PHONY: build
build: ## cargo build (release) for server + gateway + cli.
	cargo build --release \
		-p vectordb-server \
		-p vectordb-gateway \
		-p vectordb-cli \
		$(CARGO_FLAGS)

.PHONY: test
test: ## Run all workspace tests.
	cargo test --workspace $(CARGO_FLAGS)

.PHONY: fmt
fmt: ## Format the workspace.
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting.
	cargo fmt --all -- --check

.PHONY: clippy
clippy: ## Run clippy with warnings as errors.
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: clean
clean: ## Cargo clean.
	cargo clean

# ---------------------------------------------------------------------------
# Local (bare-metal) dev — single-node and cluster
# ---------------------------------------------------------------------------

.PHONY: run-server
run-server: ## Run vectordb-server with example.toml (single node).
	cargo run -p vectordb-server -- --config config/example.toml

.PHONY: run-gateway
run-gateway: ## Run the REST gateway pointed at the local router/server.
	VECTORDB_GRPC=$(ROUTER_GRPC) cargo run -p vectordb-gateway

.PHONY: run-single
run-single: ## Build + run server+gateway as raw binaries (1 shard, RF=1, no Docker). Ctrl-C to stop.
	./scripts/run-single.sh

.PHONY: run-single-release
run-single-release: ## Same as run-single but with --release binaries.
	MODE=release ./scripts/run-single.sh

.PHONY: run-single-clean
run-single-clean: ## run-single with a wiped data dir (fresh start).
	./scripts/run-single.sh --clean

.PHONY: run-shard-0 run-shard-1 run-router
run-shard-0: ## Run shard-0 locally (config/node-0.toml).
	cargo run -p vectordb-server -- --config config/node-0.toml
run-shard-1: ## Run shard-1 locally (config/node-1.toml).
	cargo run -p vectordb-server -- --config config/node-1.toml
run-router:  ## Run router locally (config/router.toml).
	cargo run -p vectordb-server -- --config config/router.toml
