#!/usr/bin/env bash
set -euo pipefail

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

if [ -f "./spatio-server" ] && [ -f "./bench_server" ]; then
    BIN_DIR="."
else
    cargo build --release -p spatio-server -p spatio-benchmarks --bin spatio-server --bin bench_server
    BIN_DIR="./target/release"
fi

"$BIN_DIR/spatio-server" --port 3000 > server_log.txt 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
echo "Server started with PID $SERVER_PID. Waiting for it to be ready..."

sleep 5

echo "Running benchmarks..."
"$BIN_DIR/bench_server" --addr "127.0.0.1:3000" --json "$OUTPUT_PATH" -n 100000 -c 100

echo "------------------------------------------------"
echo "Results saved to: $OUTPUT_PATH"
echo "------------------------------------------------"
echo "To push results to the repository:"
echo "  git pull"
echo "  git add $OUTPUT_PATH"
echo "  git commit -m \"chore: add server benchmark results for RPi 5 ($TAG)\""
echo "  git push"
echo "------------------------------------------------"
