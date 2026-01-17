#!/bin/bash
set -e

# Script to run Spatio SERVER benchmarks natively on Raspberry Pi 5
# Usage: ./scripts/run_rpi_server_bench.sh [optional-tag]

TAG=${1:-"server_manual"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="results"
FILENAME="rpi5_server_${TAG}_${TIMESTAMP}.json"
OUTPUT_PATH="${RESULT_DIR}/${FILENAME}"

mkdir -p "$RESULT_DIR"

echo "Running SERVER benchmarks on Raspberry Pi 5..."
echo "Results will be saved to: $OUTPUT_PATH"

echo "Starting Spatio Server..."

if [ -f "./spatio-server" ]; then
    SERVER_BIN="./spatio-server"
else
    SERVER_BIN="cargo run --release -p spatio-server --bin spatio-server --"
fi

# Starting server
$SERVER_BIN --port 3000 > server_log.txt 2>&1 &
SERVER_PID=$!
echo "Server started with PID $SERVER_PID. Waiting for it to be ready..."

sleep 5

echo "Running benchmarks..."
if [ -f "./bench_server" ]; then
    ./bench_server --addr "127.0.0.1:3000" --json "$OUTPUT_PATH" -n 100000 -c 100
else
    cargo run --release -p spatio-benchmarks --bin bench_server -- --addr "127.0.0.1:3000" --json "$OUTPUT_PATH" -n 100000 -c 100
fi

echo "Benchmark complete. Stopping server..."
kill $SERVER_PID

echo "------------------------------------------------"
echo "Results saved to: $OUTPUT_PATH"
echo "------------------------------------------------"
echo "To push results to the repository:"
echo "  git pull"
echo "  git add $OUTPUT_PATH"
echo "  git commit -m \"chore: add server benchmark results for RPi 5 ($TAG)\""
echo "  git push"
echo "------------------------------------------------"
