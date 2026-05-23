SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE       ?= docker compose
GATEWAY_URL   ?= http://127.0.0.1:8080
ROUTER_GRPC   ?= http://127.0.0.1:6333
CARGO_FLAGS   ?=

# ---------------------------------------------------------------------------
# Help (default target)
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*##"; printf "Available targets:\n\n"} \
		/^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' \
		$(MAKEFILE_LIST)

# ---------------------------------------------------------------------------
# macOS / filesystem hygiene
# ---------------------------------------------------------------------------

.PHONY: clean-macos
clean-macos: ## Strip macOS AppleDouble (._*) and .DS_Store files that break Docker builds.
	@find . -name '._*' -type f -delete 2>/dev/null || true
	@find . -name '.DS_Store' -type f -delete 2>/dev/null || true
	@command -v dot_clean >/dev/null 2>&1 && dot_clean -m . || true
	@echo "macOS artifacts cleaned."

# ---------------------------------------------------------------------------
# Docker / docker-compose
# ---------------------------------------------------------------------------

.PHONY: docker-build
docker-build: clean-macos ## Build all docker-compose images (cleans ._* first).
	$(COMPOSE) build

.PHONY: docker-rebuild
docker-rebuild: clean-macos ## Force a no-cache rebuild of all images.
	$(COMPOSE) build --no-cache

.PHONY: up
up: clean-macos ## Start the stack in the background (builds if needed).
	$(COMPOSE) up -d --build

.PHONY: up-fg
up-fg: clean-macos ## Start the stack in the foreground (attached).
	$(COMPOSE) up --build

.PHONY: down
down: ## Stop the stack (keeps volumes).
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop the stack and delete data volumes (DESTRUCTIVE).
	$(COMPOSE) down -v

.PHONY: restart
restart: down up ## Restart the stack.

.PHONY: ps
ps: ## Show container status.
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs from all services.
	$(COMPOSE) logs -f --tail=100

.PHONY: logs-router logs-gateway logs-shard-0 logs-shard-1
logs-router:   ## Tail router logs.
	$(COMPOSE) logs -f --tail=100 router
logs-gateway:  ## Tail gateway logs.
	$(COMPOSE) logs -f --tail=100 gateway
logs-shard-0:  ## Tail shard-0 logs.
	$(COMPOSE) logs -f --tail=100 shard-0
logs-shard-1:  ## Tail shard-1 logs.
	$(COMPOSE) logs -f --tail=100 shard-1

.PHONY: sh-router sh-gateway
sh-router:     ## Exec a shell inside the router container.
	$(COMPOSE) exec router /bin/bash
sh-gateway:    ## Exec a shell inside the gateway container.
	$(COMPOSE) exec gateway /bin/bash

# ---------------------------------------------------------------------------
# Smoke test against the gateway
# ---------------------------------------------------------------------------

SMOKE_COLLECTION ?= smoke-$(shell date +%s)

.PHONY: smoke
smoke: ## End-to-end smoke test against the running gateway (uses a unique collection name).
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
build: ## cargo build (release) for server + gateway.
	cargo build --release -p vectordb-server -p vectordb-gateway $(CARGO_FLAGS)

.PHONY: test
test: ## Run all workspace tests.
	cargo test --workspace $(CARGO_FLAGS)

.PHONY: fmt
fmt: ## Format the workspace.
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting (CI-friendly).
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

.PHONY: run-shard-0 run-shard-1 run-router
run-shard-0: ## Run shard-0 locally (config/node-0.toml).
	cargo run -p vectordb-server -- --config config/node-0.toml
run-shard-1: ## Run shard-1 locally (config/node-1.toml).
	cargo run -p vectordb-server -- --config config/node-1.toml
run-router:  ## Run router locally (config/router.toml, uses 127.0.0.1).
	cargo run -p vectordb-server -- --config config/router.toml
