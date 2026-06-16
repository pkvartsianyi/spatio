#!/usr/bin/env bash
#
# Run the core benchmark for a release, store the results, and compare them
# against the previous version's stored results.
#
# Usage:
#   scripts/bench-release.sh <version> [--dataset N] [--runs N]
#
# Produces (under crates/benchmarks/results/):
#   core-v<version>.json   raw metrics from bench_core
#   core-v<version>.md     summary + comparison vs the previous version
#
# The .md path is printed on the last line of stdout so callers (e.g. the
# release flow) can capture it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$ROOT_DIR/crates/benchmarks/results"

DATASET=100000
RUNS=5
VERSION=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) DATASET="$2"; shift 2 ;;
        --runs)    RUNS="$2"; shift 2 ;;
        -h|--help)
            sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        -*) echo "Unknown option: $1" >&2; exit 1 ;;
        *)
            if [[ -z "$VERSION" ]]; then VERSION="$1"; else
                echo "Unexpected argument: $1" >&2; exit 1
            fi
            shift
            ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    echo "ERROR: version is required (e.g. scripts/bench-release.sh 0.3.7)" >&2
    exit 1
fi
VERSION="${VERSION#v}"

mkdir -p "$RESULTS_DIR"
NEW_JSON="$RESULTS_DIR/core-v$VERSION.json"
NEW_MD="$RESULTS_DIR/core-v$VERSION.md"

cd "$ROOT_DIR"

# Find the previous version's results: the highest stored version that is not
# the one we're producing now.
PREV_JSON=""
PREV_VERSION=""
if compgen -G "$RESULTS_DIR/core-v*.json" > /dev/null; then
    while IFS= read -r f; do
        v="$(basename "$f")"; v="${v#core-v}"; v="${v%.json}"
        [[ "$v" == "$VERSION" ]] && continue
        PREV_JSON="$f"
        PREV_VERSION="$v"
    done < <(
        for f in "$RESULTS_DIR"/core-v*.json; do
            b="$(basename "$f")"; b="${b#core-v}"; b="${b%.json}"
            echo "$b $f"
        done | sort -V | awk '{print $2}'
    )
fi

echo ">>> Benchmarking spatio core v$VERSION (dataset=$DATASET, runs=$RUNS)..." >&2
cargo run -p spatio-benchmarks --bin bench_core --release -- \
    -n "$DATASET" -r "$RUNS" --json "$NEW_JSON" -q >&2

if [[ -n "$PREV_JSON" ]]; then
    echo ">>> Comparing against previous results: core-v$PREV_VERSION" >&2
else
    echo ">>> No previous results found; writing baseline report." >&2
fi

python3 "$SCRIPT_DIR/bench_compare.py" \
    --version "$VERSION" \
    --new "$NEW_JSON" \
    ${PREV_JSON:+--prev "$PREV_JSON" --prev-version "$PREV_VERSION"} \
    --out "$NEW_MD" >&2

echo ">>> Wrote $NEW_JSON" >&2
echo ">>> Wrote $NEW_MD" >&2
# Last line of stdout: the markdown path (for the caller to capture).
echo "$NEW_MD"
