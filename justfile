default:
    @just --list

# Rust commands
# =============

build:
    cargo build -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-cabi --release

test *args:
    cargo test -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-cabi -p spatio-integration-tests --all-features -- {{args}}

test-integration *args:
    cargo test -p spatio-integration-tests --all-features -- {{args}}

lint:
    cargo fmt --all
    cargo clippy -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-py -p spatio-cabi --all-targets --all-features -- -D warnings

ci:
    act -W .github/workflows/ci.yml -j test

clean:
    cargo clean

doc:
    cargo doc -p spatio -p spatio-types -p spatio-server -p spatio-client --no-deps --all-features --open

# Python commands (delegate to py-spatio)
# ======================================

py-setup:
    cd bindings/python && just setup

py-build:
    cd bindings/python && just build

py-build-release:
    cd bindings/python && just build-release

py-test:
    cd bindings/python && just test

py-coverage:
    cd bindings/python && just coverage

py-fmt:
    cd bindings/python && just fmt

py-lint:
    cd bindings/python && just lint

py-typecheck:
    cd bindings/python && just typecheck

py-examples:
    cd bindings/python && just examples

py-example name:
    cd bindings/python && just example {{name}}

py-wheel:
    cd bindings/python && just wheel

py-clean:
    cd bindings/python && just clean

py-bench:
    cd bindings/python && just bench

py-version:
    cd bindings/python && just version

py-dev-setup:
    cd bindings/python && just dev-setup

py-ci:
    cd bindings/python && just ci

# Go commands (purego bindings)
# =============================

# Build the C-ABI cdylib and stage it under bindings/go/libs/<goos>_<goarch>/.
go-build-lib:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p spatio-cabi --release
    os=$(go env GOOS); arch=$(go env GOARCH)
    case "$os" in
      darwin) file=libspatio_cabi.dylib;;
      *)      file=libspatio_cabi.so;;
    esac
    dest="bindings/go/libs/${os}_${arch}"
    mkdir -p "$dest"
    cp "target/release/${file}" "$dest/"
    echo "staged ${file} -> ${dest}"

go-test: go-build-lib
    cd bindings/go && go test ./...

go-vet:
    cd bindings/go && go vet ./...

go-fmt:
    cd bindings/go && gofmt -w .

go-example: go-build-lib
    cd bindings/go && go run ./examples/basic

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

# Bump patch versions for types, core, server, client, python (dependency order).
# Runs the core release benchmark and includes its results in the commit.
patch-all:
    #!/usr/bin/env bash
    set -e

    # Get current versions
    TYPES_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-types","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    CORE_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    SERVER_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-server","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    CLIENT_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-client","version":"[^"]*"' | head -1 | cut -d'"' -f8)
    PYTHON_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | grep -o '"name":"spatio-py","version":"[^"]*"' | head -1 | cut -d'"' -f8)

    # Function to bump patch version
    bump_patch() {
        local version=$1
        local major=$(echo "$version" | cut -d. -f1)
        local minor=$(echo "$version" | cut -d. -f2)
        local patch=$(echo "$version" | cut -d. -f3 | cut -d- -f1)
        echo "$major.$minor.$((patch + 1))"
    }

    NEW_TYPES=$(bump_patch "$TYPES_VERSION")
    NEW_CORE=$(bump_patch "$CORE_VERSION")
    NEW_SERVER=$(bump_patch "$SERVER_VERSION")
    NEW_CLIENT=$(bump_patch "$CLIENT_VERSION")
    NEW_PYTHON=$(bump_patch "$PYTHON_VERSION")

    echo "=== Patch Version Bump ==="
    echo "  types:  $TYPES_VERSION -> $NEW_TYPES"
    echo "  core:   $CORE_VERSION -> $NEW_CORE"
    echo "  server: $SERVER_VERSION -> $NEW_SERVER"
    echo "  client: $CLIENT_VERSION -> $NEW_CLIENT"
    echo "  python: $PYTHON_VERSION -> $NEW_PYTHON"
    echo ""

    # Bump in dependency order: types > core > server > client > python.
    # The core bump also runs the release benchmark (crates/benchmarks/results/).
    echo "=== Bumping types ==="
    ./scripts/bump-version.sh types "$NEW_TYPES" --no-commit

    echo ""
    echo "=== Bumping core ==="
    ./scripts/bump-version.sh core "$NEW_CORE" --no-commit

    echo ""
    echo "=== Bumping server ==="
    ./scripts/bump-version.sh server "$NEW_SERVER" --no-commit

    echo ""
    echo "=== Bumping client ==="
    ./scripts/bump-version.sh client "$NEW_CLIENT" --no-commit

    echo ""
    echo "=== Bumping python ==="
    ./scripts/bump-version.sh python "$NEW_PYTHON" --no-commit

    echo ""
    echo "=== Committing changes ==="
    git add crates/types/Cargo.toml crates/core/Cargo.toml crates/server/Cargo.toml crates/client/Cargo.toml bindings/python/Cargo.toml Cargo.toml Cargo.lock
    # Include the benchmark results produced by the core bump.
    if [ -f "crates/benchmarks/results/core-v$NEW_CORE.json" ]; then
        git add "crates/benchmarks/results/core-v$NEW_CORE.json" "crates/benchmarks/results/core-v$NEW_CORE.md"
    fi
    git commit -m "bump: types $NEW_TYPES, core $NEW_CORE, server $NEW_SERVER, client $NEW_CLIENT, python $NEW_PYTHON"

    echo ""
    echo "=== Done! ==="
    echo "Push to main to trigger releases."
    echo "CI will build in order: types > core > server > client; python publishes to PyPI"

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
    cd bindings/python && bandit -r src/ && safety check

bench-core *args:
    cargo run -p spatio-benchmarks --bin bench_core --release -- {{args}}

# Run the core release benchmark, store results, and compare to the previous
# version. Runs automatically as part of `just bump-core`.
bench-release VERSION:
    ./scripts/bench-release.sh {{VERSION}}

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
    cd bindings/python && just coverage

test-examples:
    cargo run -p spatio --example getting_started
    cargo run -p spatio --example spatial_queries
    cargo run -p spatio --example trajectory_tracking
    cargo run -p spatio --example 3d_spatial_tracking
    cd bindings/python && just examples

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

