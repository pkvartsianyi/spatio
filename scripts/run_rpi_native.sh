#!/bin/bash
set -e

# Script to run Spatio benchmarks natively on Raspberry Pi 5
# Usage: ./scripts/run_rpi_native.sh [optional-tag]

TAG=${1:-"manual"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="results"
FILENAME="rpi5_${TAG}_${TIMESTAMP}.json"
OUTPUT_PATH="${RESULT_DIR}/${FILENAME}"

mkdir -p "$RESULT_DIR"

echo "Running benchmarks on Raspberry Pi 5..."
echo "Results will be saved to: $OUTPUT_PATH"

# Run bench_core with JSON output
# Assuming we are running from root of the repo
cargo run --release -p spatio-benchmarks --bin bench_core -- --json "$OUTPUT_PATH" -n 100000 -r 5

echo "Benchmark complete!"
echo "------------------------------------------------"
echo "To push results to the repository:"
echo "  git pull"
echo "  git add $OUTPUT_PATH"
echo "  git commit -m \"chore: add benchmark results for RPi 5 ($TAG)\""
echo "  git push"
echo "------------------------------------------------"
