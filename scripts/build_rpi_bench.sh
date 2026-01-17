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
cargo zigbuild --target "$TARGET" --release -p spatio-benchmarks --bin bench_core

echo "Build complete!"
echo "Binary location: $OUTPUT_DIR/bench_core"
echo "Transfer this binary to your RPi 5 to run benchmarks."
