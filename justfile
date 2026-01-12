default:
    @just --list

# Rust commands
# =============

build:
    cargo build -p spatio -p spatio-types -p spatio-server -p spatio-client --release

test *args:
    cargo test -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-integration-tests --all-features -- {{args}}

test-integration *args:
    cargo test -p spatio-integration-tests --all-features -- {{args}}

lint:
    cargo fmt --all
    cargo clippy -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-py --all-targets --all-features -- -D warnings

ci:
    act -W .github/workflows/ci.yml -j test

clean:
    cargo clean

doc:
    cargo doc -p spatio -p spatio-types -p spatio-server -p spatio-client --no-deps --all-features --open

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


bump-client VERSION:
    ./scripts/bump-version.sh client {{VERSION}}

# Bump patch versions for core, server, client (in dependency order)
patch-all:
    #!/usr/bin/env bash
    set -e
    
    # Get current versions
    CORE_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    SERVER_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-server","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    CLIENT_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-client","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    
    # Function to bump patch version
    bump_patch() {
        local version=$1
        local major=$(echo "$version" | cut -d. -f1)
        local minor=$(echo "$version" | cut -d. -f2)
        local patch=$(echo "$version" | cut -d. -f3 | cut -d- -f1)
        echo "$major.$minor.$((patch + 1))"
    }
    
    NEW_CORE=$(bump_patch "$CORE_VERSION")
    NEW_SERVER=$(bump_patch "$SERVER_VERSION")
    NEW_CLIENT=$(bump_patch "$CLIENT_VERSION")
    
    echo "=== Patch Version Bump ==="
    echo "  core:   $CORE_VERSION -> $NEW_CORE"
    echo "  server: $SERVER_VERSION -> $NEW_SERVER"
    echo "  client: $CLIENT_VERSION -> $NEW_CLIENT"
    echo ""
    
    # Bump in dependency order: core > server > client
    echo "=== Bumping core ==="
    ./scripts/bump-version.sh core "$NEW_CORE" --no-commit
    
    echo ""
    echo "=== Bumping server ==="
    ./scripts/bump-version.sh server "$NEW_SERVER" --no-commit
    
    echo ""
    echo "=== Bumping client ==="
    ./scripts/bump-version.sh client "$NEW_CLIENT" --no-commit
    
    echo ""
    echo "=== Committing changes ==="
    git add crates/core/Cargo.toml crates/server/Cargo.toml crates/client/Cargo.toml Cargo.toml Cargo.lock
    git commit -m "bump: core $NEW_CORE, server $NEW_SERVER, client $NEW_CLIENT"
    
    echo ""
    echo "=== Done! ===" 
    echo "Push to main to trigger releases."
    echo "CI will build in order: core > server > client"

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

    cargo bench -p spatio -p spatio-server -p spatio-client
    cd crates/py && just bench

bench-core *args:
    cargo run -p spatio-benchmarks --bin bench_core --release -- {{args}}

bench-all:
    @echo "=== CORE BENCHMARKS ==="
    just bench-core -q
    @echo ""
    @echo "=== SERVER BENCHMARKS ==="
    just bench-server

bench-server:
    #!/usr/bin/env bash
    set -e
    echo "Building release binaries..."
    cargo build -p spatio-server -p spatio-benchmarks --release --quiet

    pkill spatio-server || true

    echo "Starting optimized server (RUST_LOG=error)..."
    RUST_LOG=error ./target/release/spatio-server --port 3000 > /dev/null 2>&1 &
    SERVER_PID=$!

    trap "kill $SERVER_PID" EXIT

    sleep 3

    echo "Running benchmark..."
    ./target/release/bench_server -q

coverage:
    cargo tarpaulin --verbose --all-features -p spatio -p spatio-types -p spatio-server -p spatio-client --timeout 120 --out html
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

# Docker commands
# ===============

docker-build-server:
    docker build -f crates/server/Dockerfile -t spatio-server:latest .

docker-run-server:
    docker run -it --rm -p 3000:3000 spatio-server:latest

