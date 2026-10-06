default:
    @just --list

# Rust commands
# =============

build:
    cargo build -p spatio -p spatio-types -p spatio-server -p spatio-client -p spatio-cabi --release

test *args:
    cargo test --workspace --all-features --exclude spatio-py -- {{args}}

test-integration *args:
    cargo test -p spatio-integration-tests --all-features -- {{args}}

lint:
    cargo fmt --all
    cargo clippy --workspace --all-targets --all-features -- -D warnings

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

# Version management (requires cargo-edit for `cargo set-version`)
# ==================

# Set a crate's version, e.g. `just bump spatio 0.3.10`. Bumping spatio runs the release benchmark.
bump CRATE VERSION:
    cargo set-version -p {{CRATE}} {{VERSION}}
    if [ "{{CRATE}}" = spatio ]; then ./scripts/bench-release.sh {{VERSION}}; fi

bump-go VERSION:
    echo "{{VERSION}}" > bindings/go/VERSION

# Bump the patch version of every crate and the Go bindings, then run the release benchmark.
patch-all:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo set-version --bump patch -p spatio-types -p spatio -p spatio-server -p spatio-client -p spatio-py -p spatio-cabi
    go=$(tr -d '[:space:]' < bindings/go/VERSION)
    echo "${go%.*}.$((${go##*.} + 1))" > bindings/go/VERSION
    id=$(cargo pkgid -p spatio)
    ./scripts/bench-release.sh "${id##*@}"

# CI and Testing
# ==============

security-audit:
    cargo audit
    cd bindings/python && just security

bench-core *args:
    cargo run -p spatio-benchmarks --bin bench_core --release -- {{args}}

# Run the core release benchmark and compare to the previous version.
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

