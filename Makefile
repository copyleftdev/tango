# ═══════════════════════════════════════════════════════════════════════════════
#  TANGO - High-Performance Multi-Format Log Parser
#  Makefile for build, install, test, and uninstall
# ═══════════════════════════════════════════════════════════════════════════════

BINARY_NAME := tango
VERSION := $(shell grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
PREFIX := /usr/local
BINDIR := $(PREFIX)/bin
MANDIR := $(PREFIX)/share/man/man1

CARGO := cargo
CARGO_FLAGS := --release

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m
BOLD := \033[1m

.PHONY: all build release debug test test-unit test-integration bench clean install uninstall help check fmt lint doc

# ═══════════════════════════════════════════════════════════════════════════════
#  DEFAULT TARGET
# ═══════════════════════════════════════════════════════════════════════════════

all: build

# ═══════════════════════════════════════════════════════════════════════════════
#  BUILD TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

build: ## Build release binary
	@printf "$(CYAN)$(BOLD)═══ Building Tango v$(VERSION) ═══$(RESET)\n"
	@$(CARGO) build $(CARGO_FLAGS)
	@printf "$(GREEN)✓ Build complete: target/release/$(BINARY_NAME)$(RESET)\n"

release: build ## Alias for build

debug: ## Build debug binary
	@printf "$(CYAN)$(BOLD)═══ Building Tango (debug) ═══$(RESET)\n"
	@$(CARGO) build
	@printf "$(GREEN)✓ Debug build complete: target/debug/$(BINARY_NAME)$(RESET)\n"

# ═══════════════════════════════════════════════════════════════════════════════
#  TEST TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

test: test-unit test-cli ## Run all tests
	@printf "$(GREEN)$(BOLD)✓ All tests passed!$(RESET)\n"

test-unit: ## Run unit tests
	@printf "$(CYAN)$(BOLD)═══ Running Unit Tests ═══$(RESET)\n"
	@$(CARGO) test --lib
	@printf "$(GREEN)✓ Unit tests passed$(RESET)\n"

test-cli: build ## Run CLI integration tests
	@printf "$(CYAN)$(BOLD)═══ Running CLI Tests ═══$(RESET)\n"
	@printf "Testing parse command... "
	@./target/release/$(BINARY_NAME) parse test_data/Apache/Apache_2k.log -n 1 > /dev/null && printf "$(GREEN)✓$(RESET)\n" || (printf "$(RED)✗$(RESET)\n" && exit 1)
	@printf "Testing search command... "
	@./target/release/$(BINARY_NAME) search test_data/OpenSSH/OpenSSH_2k.log --grep "Failed" -n 1 > /dev/null && printf "$(GREEN)✓$(RESET)\n" || (printf "$(RED)✗$(RESET)\n" && exit 1)
	@printf "Testing stats command... "
	@./target/release/$(BINARY_NAME) stats test_data/Android/Android_2k.log > /dev/null && printf "$(GREEN)✓$(RESET)\n" || (printf "$(RED)✗$(RESET)\n" && exit 1)
	@printf "Testing tail command... "
	@./target/release/$(BINARY_NAME) tail test_data/Linux/Linux_2k.log -n 5 > /dev/null && printf "$(GREEN)✓$(RESET)\n" || (printf "$(RED)✗$(RESET)\n" && exit 1)
	@printf "Testing convert command... "
	@./target/release/$(BINARY_NAME) convert test_data/Apache/Apache_2k.log -f json > /dev/null && printf "$(GREEN)✓$(RESET)\n" || (printf "$(RED)✗$(RESET)\n" && exit 1)
	@printf "$(GREEN)✓ CLI tests passed$(RESET)\n"

bench: ## Run benchmarks
	@printf "$(CYAN)$(BOLD)═══ Running Benchmarks ═══$(RESET)\n"
	@$(CARGO) bench

# ═══════════════════════════════════════════════════════════════════════════════
#  CODE QUALITY
# ═══════════════════════════════════════════════════════════════════════════════

check: ## Check code without building
	@printf "$(CYAN)$(BOLD)═══ Checking Code ═══$(RESET)\n"
	@$(CARGO) check
	@printf "$(GREEN)✓ Check passed$(RESET)\n"

fmt: ## Format code
	@printf "$(CYAN)$(BOLD)═══ Formatting Code ═══$(RESET)\n"
	@$(CARGO) fmt
	@printf "$(GREEN)✓ Code formatted$(RESET)\n"

fmt-check: ## Check code formatting
	@$(CARGO) fmt -- --check

lint: ## Run clippy linter
	@printf "$(CYAN)$(BOLD)═══ Running Clippy ═══$(RESET)\n"
	@$(CARGO) clippy $(CARGO_FLAGS) -- -D warnings
	@printf "$(GREEN)✓ Lint passed$(RESET)\n"

doc: ## Generate documentation
	@printf "$(CYAN)$(BOLD)═══ Generating Documentation ═══$(RESET)\n"
	@$(CARGO) doc --no-deps
	@printf "$(GREEN)✓ Documentation generated: target/doc/tango/index.html$(RESET)\n"

# ═══════════════════════════════════════════════════════════════════════════════
#  INSTALL / UNINSTALL
# ═══════════════════════════════════════════════════════════════════════════════

install: build ## Install tango to $(BINDIR)
	@printf "$(CYAN)$(BOLD)═══ Installing Tango v$(VERSION) ═══$(RESET)\n"
	@printf "Installing to $(BINDIR)/$(BINARY_NAME)...\n"
	@install -d $(BINDIR)
	@install -m 755 target/release/$(BINARY_NAME) $(BINDIR)/$(BINARY_NAME)
	@printf "$(GREEN)$(BOLD)✓ Tango installed successfully!$(RESET)\n"
	@printf "\nRun '$(BINARY_NAME) --help' to get started.\n"

uninstall: ## Uninstall tango from $(BINDIR)
	@printf "$(CYAN)$(BOLD)═══ Uninstalling Tango ═══$(RESET)\n"
	@rm -f $(BINDIR)/$(BINARY_NAME)
	@printf "$(GREEN)✓ Tango uninstalled from $(BINDIR)$(RESET)\n"

# ═══════════════════════════════════════════════════════════════════════════════
#  CLEAN
# ═══════════════════════════════════════════════════════════════════════════════

clean: ## Clean build artifacts
	@printf "$(CYAN)$(BOLD)═══ Cleaning Build Artifacts ═══$(RESET)\n"
	@$(CARGO) clean
	@rm -f /tmp/converted.* /tmp/merged.*
	@printf "$(GREEN)✓ Clean complete$(RESET)\n"

# ═══════════════════════════════════════════════════════════════════════════════
#  DEVELOPMENT
# ═══════════════════════════════════════════════════════════════════════════════

dev: ## Build and run with example
	@$(CARGO) run -- parse test_data/Apache/Apache_2k.log -n 10

watch: ## Watch for changes and rebuild
	@cargo watch -x 'build --release'

# ═══════════════════════════════════════════════════════════════════════════════
#  RELEASE
# ═══════════════════════════════════════════════════════════════════════════════

release-linux: ## Build optimized Linux release
	@printf "$(CYAN)$(BOLD)═══ Building Linux Release ═══$(RESET)\n"
	@RUSTFLAGS="-C target-cpu=native" $(CARGO) build --release
	@strip target/release/$(BINARY_NAME)
	@printf "$(GREEN)✓ Linux release: target/release/$(BINARY_NAME)$(RESET)\n"
	@ls -lh target/release/$(BINARY_NAME)

# ═══════════════════════════════════════════════════════════════════════════════
#  HELP
# ═══════════════════════════════════════════════════════════════════════════════

help: ## Show this help message
	@printf "$(BOLD)Tango - High-Performance Multi-Format Log Parser$(RESET)\n\n"
	@printf "$(BOLD)Usage:$(RESET)\n"
	@printf "  make $(CYAN)<target>$(RESET)\n\n"
	@printf "$(BOLD)Targets:$(RESET)\n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-15s$(RESET) %s\n", $$1, $$2}'
	@printf "\n$(BOLD)Examples:$(RESET)\n"
	@printf "  make build        # Build release binary\n"
	@printf "  make test         # Run all tests\n"
	@printf "  make install      # Install to /usr/local/bin\n"
	@printf "  make uninstall    # Remove from /usr/local/bin\n"
	@printf "\n$(BOLD)Variables:$(RESET)\n"
	@printf "  PREFIX=$(PREFIX)  # Installation prefix\n"
	@printf "  BINDIR=$(BINDIR)  # Binary installation directory\n"
