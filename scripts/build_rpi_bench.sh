#!/bin/bash
set -e

# Build script for building Spatio benchmarks for Raspberry Pi 5 (aarch64)
# Requirements: 
# - cargo-zigbuild (cargo install cargo-zigbuild)
# - aarch64-unknown-linux-gnu target (rustup target add aarch64-unknown-linux-gnu)
# - zig (usually installed via brew or package manager)

TARGET="aarch64-unknown-linux-gnu"
OUTPUT_DIR="target/$TARGET/release"

echo "Building Spatio benchmarks for $TARGET..."

# Check if cargo-zigbuild is installed
if ! command -v cargo-zigbuild &> /dev/null; then
    echo "Error: cargo-zigbuild is not installed. Please install it with:"
    echo "  cargo install cargo-zigbuild"
    exit 1
fi

# Build bench_core
echo "Building bench_core..."
cargo zigbuild --target "$TARGET" --release -p spatio-benchmarks --bin bench_core

# Build bench_server
echo "Building bench_server..."
cargo zigbuild --target "$TARGET" --release -p spatio-benchmarks --bin bench_server

# Build spatio-server
echo "Building spatio-server..."
cargo zigbuild --target "$TARGET" --release -p spatio-server --bin spatio-server

echo "Build complete!"
echo "Binaries location: $OUTPUT_DIR"
echo "  - bench_core"
echo "  - bench_server"
echo "  - spatio-server"
echo "Transfer these binaries to your RPi 5."
