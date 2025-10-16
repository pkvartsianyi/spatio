# Default recipe - show available commands
default:
    @just --list

# Rust commands
# =============

# Build in release mode
build:
    cargo build --release

# Run tests
test:
    cargo test --all

# Run formatter and linter
lint:
    cargo fmt -- --check
    cargo clippy -- -D warnings

# Run the same tests as GitHub Actions
ci:
    act -j test

# Clean target directory
clean:
    cargo clean

# Generate documentation
doc:
    cargo doc --no-deps --open

# Python commands (delegate to py-spatio)
# ======================================

# Set up Python development environment
py-setup:
    cd py-spatio && just setup

# Build Python package
py-build:
    cd py-spatio && just build

# Build Python package (release)
py-build-release:
    cd py-spatio && just build-release

# Run Python tests
py-test:
    cd py-spatio && just test

# Run Python tests with coverage
py-coverage:
    cd py-spatio && just coverage

# Format Python code
py-fmt:
    cd py-spatio && just fmt

# Lint Python code
py-lint:
    cd py-spatio && just lint

# Run Python type checking
py-typecheck:
    cd py-spatio && just typecheck

# Run Python examples
py-examples:
    cd py-spatio && just examples

# Run specific Python example
py-example name:
    cd py-spatio && just example {{name}}

# Build Python wheel
py-wheel:
    cd py-spatio && just wheel

# Clean Python artifacts
py-clean:
    cd py-spatio && just clean

# Run Python benchmarks
py-bench:
    cd py-spatio && just bench

# Show Python package version
py-version:
    cd py-spatio && just version

# Python development setup
py-dev-setup:
    cd py-spatio && just dev-setup

# Run Python CI pipeline
py-ci:
    cd py-spatio && just ci

# Combined commands
# ================

# Run all tests (Rust + Python)
test-all: test py-test

# Format all code (Rust + Python)
fmt-all: py-fmt
    cargo fmt

# Lint all code (Rust + Python)
lint-all: lint py-lint

# Clean everything (Rust + Python)
clean-all: clean py-clean

# Full CI for both Rust and Python
ci-all: ci py-ci
