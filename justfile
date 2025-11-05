default:
    @just --list

# Rust commands
# =============

build:
    cargo build --release

test:
    cargo test --all

lint:
    cargo fmt
    cargo clippy --all-targets --all-features -- -D warnings

ci:
    act -W .github/workflows/ci.yml -j test

ci-fake-release:
    @echo "Running auto-release workflow locally (dry-run mode)..."
    @echo "Note: Only running detect-changes and test-rust jobs to avoid platform issues"
    act -W .github/workflows/auto-release.yml --env DRY_RUN=true --container-architecture linux/amd64 -j detect-changes -j test-rust || true

clean:
    cargo clean

doc:
    cargo doc --no-deps --open

# Python commands (delegate to py-spatio)
# ======================================

py-setup:
    cd py-spatio && just setup

py-build:
    cd py-spatio && just build

py-build-release:
    cd py-spatio && just build-release

py-test:
    cd py-spatio && just test

py-coverage:
    cd py-spatio && just coverage

py-fmt:
    cd py-spatio && just fmt

py-lint:
    cd py-spatio && just lint

py-typecheck:
    cd py-spatio && just typecheck

py-examples:
    cd py-spatio && just examples

py-example name:
    cd py-spatio && just example {{name}}

py-wheel:
    cd py-spatio && just wheel

py-clean:
    cd py-spatio && just clean

py-bench:
    cd py-spatio && just bench

py-version:
    cd py-spatio && just version

py-dev-setup:
    cd py-spatio && just dev-setup

py-ci:
    cd py-spatio && just ci

# Version management
# ==================

check-version:
    ./scripts/check-version.sh

bump-rust VERSION:
    ./scripts/bump-version.sh rust {{VERSION}}

bump-python VERSION:
    ./scripts/bump-version.sh python {{VERSION}}

bump-types VERSION:
    ./scripts/bump-version.sh types {{VERSION}}

bump-all VERSION:
    ./scripts/bump-version.sh all {{VERSION}}

bump-rust-dry VERSION:
    ./scripts/bump-version.sh rust {{VERSION}} --dry-run

bump-python-dry VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --dry-run

bump-types-dry VERSION:
    ./scripts/bump-version.sh types {{VERSION}} --dry-run

bump-all-dry VERSION:
    ./scripts/bump-version.sh all {{VERSION}} --dry-run

bump-rust-no-commit VERSION:
    ./scripts/bump-version.sh rust {{VERSION}} --no-commit

bump-python-no-commit VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --no-commit

# CI and Testing
# ==============

security-audit:
    cargo audit
    cd py-spatio && bandit -r src/ && safety check

benchmarks:
    cargo bench
    cd py-spatio && just bench

coverage:
    cargo tarpaulin --verbose --all-features --workspace --timeout 120 --out html
    cd py-spatio && just coverage

test-examples:
    cargo run --example getting_started
    cargo run --example spatial_queries
    cargo run --example trajectory_tracking
    cargo run --example comprehensive_demo
    cd py-spatio && just examples

# Combined commands
# ================

test-all: test py-test

fmt-all: py-fmt
    cargo fmt

lint-all: lint py-lint

clean-all: clean py-clean

ci-all: ci py-ci
