default:
    @just --list

# Rust commands
# =============

build:
    cargo build -p spatio -p spatio-types -p spatio-server --release

test:
    cargo test -p spatio -p spatio-types -p spatio-server --all-features

lint:
    cargo fmt --all
    cargo clippy -p spatio -p spatio-types -p spatio-server -p spatio-py --all-targets --all-features -- -D warnings

ci:
    act -W .github/workflows/ci.yml -j test

clean:
    cargo clean

doc:
    cargo doc -p spatio -p spatio-types -p spatio-server --no-deps --all-features --open

# Python commands (delegate to py-spatio)
# ======================================

py-setup:
    cd crates/py && just setup

py-build:
    cd crates/py && just build

py-build-release:
    cd crates/py && just build-release

py-test:
    cd crates/py && just test

py-coverage:
    cd crates/py && just coverage

py-fmt:
    cd crates/py && just fmt

py-lint:
    cd crates/py && just lint

py-typecheck:
    cd crates/py && just typecheck

py-examples:
    cd crates/py && just examples

py-example name:
    cd crates/py && just example {{name}}

py-wheel:
    cd crates/py && just wheel

py-clean:
    cd crates/py && just clean

py-bench:
    cd crates/py && just bench

py-version:
    cd crates/py && just version

py-dev-setup:
    cd crates/py && just dev-setup

py-ci:
    cd crates/py && just ci

# Version management
# ==================

check-version:
    ./scripts/check-version.sh

bump-core VERSION:
    ./scripts/bump-version.sh core {{VERSION}}

bump-python VERSION:
    ./scripts/bump-version.sh python {{VERSION}}

bump-types VERSION:
    ./scripts/bump-version.sh types {{VERSION}}

bump-server VERSION:
    ./scripts/bump-version.sh server {{VERSION}}

bump-core-dry VERSION:
    ./scripts/bump-version.sh core {{VERSION}} --dry-run

bump-python-dry VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --dry-run

bump-types-dry VERSION:
    ./scripts/bump-version.sh types {{VERSION}} --dry-run

bump-server-dry VERSION:
    ./scripts/bump-version.sh server {{VERSION}} --dry-run

bump-core-no-commit VERSION:
    ./scripts/bump-version.sh core {{VERSION}} --no-commit

bump-python-no-commit VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --no-commit

# CI and Testing
# ==============

security-audit:
    cargo audit
    cd crates/py && bandit -r src/ && safety check

benchmarks:
    cargo bench -p spatio -p spatio-server
    cd crates/py && just bench

coverage:
    cargo tarpaulin --verbose --all-features -p spatio -p spatio-types -p spatio-server --timeout 120 --out html
    cd crates/py && just coverage

test-examples:
    cargo run -p spatio --example getting_started
    cargo run -p spatio --example spatial_queries
    cargo run -p spatio --example trajectory_tracking
    cargo run -p spatio --example 3d_spatial_tracking
    cd crates/py && just examples

# Combined commands
# ================

test-all: test py-test

fmt-all: py-fmt
    cargo fmt

lint-all: lint py-lint

clean-all: clean py-clean

ci-all: ci py-ci
