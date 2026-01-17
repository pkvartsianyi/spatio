#!/bin/bash
set -e

echo "Building benchmarks natively..."

# Build bench_core
echo "Building bench_core..."
cargo build --release -p spatio-benchmarks --bin bench_core

# Build bench_server
echo "Building bench_server..."
cargo build --release -p spatio-benchmarks --bin bench_server

# Build spatio-server
echo "Building spatio-server..."
cargo build --release -p spatio-server --bin spatio-server

echo "------------------------------------------------"
echo "Build complete! Artifacts are in target/release/:"
echo "  - target/release/bench_core"
echo "  - target/release/bench_server"
echo "  - target/release/spatio-server"
echo "------------------------------------------------"
