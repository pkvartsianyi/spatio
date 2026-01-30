#!/bin/bash
set -e

# Detect Platform
OS="$(uname -s)"
PLATFORM="unknown"

if [ "$OS" = "Darwin" ]; then
    PLATFORM="mac"
elif [ "$OS" = "Linux" ]; then
    # Try to detect Raspberry Pi 5
    if [ -f /proc/device-tree/model ] && grep -q "Raspberry Pi 5" /proc/device-tree/model; then
        PLATFORM="rpi5"
    else
        # Fallback for other Linux/RPi if exact model match fails, assuming user context
        PLATFORM="rpi5" 
    fi
fi

echo "Detected platform: $PLATFORM"

RESULTS_DIR="results"
mkdir -p "$RESULTS_DIR"

CORE_FILE="${RESULTS_DIR}/${PLATFORM}_core.json"
SERVER_FILE="${RESULTS_DIR}/${PLATFORM}_server.json"

echo "=== Building Benchmarks ==="
cargo build --release -p spatio-benchmarks -p spatio-server --quiet

echo "=== Running Core Benchmarks ==="
# Using defaults (N=100,000)
./target/release/bench_core -q --json "$CORE_FILE"

echo "=== Running Server Benchmarks ==="
# Ensure no existing server is running
pkill spatio-server || true

# Start server in background
echo "Starting Spatio Server..."
RUST_LOG=error ./target/release/spatio-server --port 3000 > /dev/null 2>&1 &
SERVER_PID=$!

# Ensure server is killed on script exit
trap "kill $SERVER_PID" EXIT

# Give server time to start
sleep 3

echo "Running Client Benchmarks..."
./target/release/bench_server -q --json "$SERVER_FILE"

echo "------------------------------------------------"
echo "Benchmarks Complete."
echo "Core results:   $CORE_FILE"
echo "Server results: $SERVER_FILE"
