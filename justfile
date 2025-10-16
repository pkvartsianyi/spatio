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
