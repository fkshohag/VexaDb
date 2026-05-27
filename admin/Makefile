# Convenience passthrough so the admin folder doesn't surprise people who run
# `make` here. Every target forwards to the root Makefile so there is exactly
# one source of truth for build / lifecycle commands.

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show targets (forwards to repo root).
	@echo "Targets here forward to $(ROOT)/Makefile. Common ones:"
	@echo
	@echo "  make up-all           # bare-metal server + gateway + admin"
	@echo "  make up-all-clean     # fresh data dir"
	@echo "  make up-all-release   # --release binaries"
	@echo "  make down-all         # stop the stack"
	@echo "  make admin-build      # build only the admin (frontend + Go)"
	@echo "  make admin-dev        # dev mode: Go + Vite with HMR"
	@echo "  make admin-open       # open the panel in your browser"
	@echo
	@echo "Anything else passes through, e.g. \`make ps\`, \`make logs\`, \`make help\`."

# Forwarded targets. Listed explicitly (rather than a catch-all `%:` rule) so
# tab-completion in zsh/bash still works and typos fail loudly.
FORWARDED := \
	up-all up-all-clean up-all-release down-all \
	up up-clean up-release down nuke ps logs render restart info \
	scale-rf add-shard \
	admin-build admin-dev admin-open logs-admin \
	run-single run-single-release run-single-clean \
	build check test fmt fmt-check clippy clean smoke \
	clean-macos

.PHONY: $(FORWARDED)
$(FORWARDED):
	@$(MAKE) -C "$(ROOT)" $@
