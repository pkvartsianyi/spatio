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

# Version management
# ==================

# Check version status of all packages
check-version:
    ./scripts/check-version.sh

# Bump Rust crate version
bump-rust VERSION:
    ./scripts/bump-version.sh rust {{VERSION}}

# Bump Python package version
bump-python VERSION:
    ./scripts/bump-version.sh python {{VERSION}}

# Bump both packages to same version
bump-both VERSION:
    ./scripts/bump-version.sh both {{VERSION}}

# Dry run version bump to see what would change
bump-rust-dry VERSION:
    ./scripts/bump-version.sh rust {{VERSION}} --dry-run

bump-python-dry VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --dry-run

bump-both-dry VERSION:
    ./scripts/bump-version.sh both {{VERSION}} --dry-run

# Bump version without committing
bump-rust-no-commit VERSION:
    ./scripts/bump-version.sh rust {{VERSION}} --no-commit

bump-python-no-commit VERSION:
    ./scripts/bump-version.sh python {{VERSION}} --no-commit

# CI and Testing
# ==============

# Run security audit
security-audit:
    cargo audit
    cd py-spatio && bandit -r src/ && safety check

# Run performance benchmarks
benchmarks:
    cargo bench
    cd py-spatio && just bench

# Run code coverage
coverage:
    cargo tarpaulin --verbose --all-features --workspace --timeout 120 --out html
    cd py-spatio && just coverage

# Run all examples to test functionality
test-examples:
    cargo run --example getting_started
    cargo run --example spatial_queries
    cargo run --example trajectory_tracking
    cargo run --example comprehensive_demo
    cd py-spatio && just examples

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
