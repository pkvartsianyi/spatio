#!/usr/bin/env python3
"""Aggregate bench_core JSON output and render a release comparison report.

Reads the raw per-run metrics emitted by `bench_core --json`, averages them per
operation, and writes a markdown summary. When a previous version's results are
supplied, each operation is compared and the percentage change is shown.
"""

from __future__ import annotations

import argparse
import json
from collections import OrderedDict

# A throughput drop larger than this (fractional) is flagged as a regression.
REGRESSION_THRESHOLD = 0.05


def aggregate(path: str) -> "OrderedDict[str, dict]":
    """Average throughput and latency per operation from a results file."""
    with open(path) as fh:
        data = json.load(fh)

    groups: "OrderedDict[str, list]" = OrderedDict()
    for run in data["runs"]:
        groups.setdefault(run["name"], []).append(run)

    ops: "OrderedDict[str, dict]" = OrderedDict()
    for name, runs in groups.items():
        throughputs = [r["ops_count"] / r["total_duration_secs"] for r in runs]
        latencies = [r["total_duration_secs"] * 1e6 / r["ops_count"] for r in runs]
        ops[name] = {
            "throughput": sum(throughputs) / len(throughputs),
            "latency_us": sum(latencies) / len(latencies),
        }
    return ops, data


def pct(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def fmt_delta(value: float, *, higher_is_better: bool) -> str:
    """Format a signed percentage, flagging meaningful regressions."""
    sign = "+" if value >= 0 else ""
    improved = value >= 0 if higher_is_better else value <= 0
    regressed = (not improved) and abs(value) >= REGRESSION_THRESHOLD * 100.0
    marker = " ⚠️" if regressed else ""
    return f"{sign}{value:.1f}%{marker}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--prev")
    ap.add_argument("--prev-version")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    new_ops, meta = aggregate(args.new)
    prev_ops = aggregate(args.prev)[0] if args.prev else None

    lines = []
    lines.append(f"## spatio core v{args.version} — benchmark results")
    lines.append("")
    lines.append(
        f"Platform `{meta.get('platform', '?')}` · "
        f"dataset {meta.get('dataset_size', '?')} · "
        f"{meta.get('timestamp', '')}"
    )
    lines.append("")

    if prev_ops is not None:
        lines.append(f"Compared against **core-v{args.prev_version}**.")
        lines.append("")
        lines.append(
            "| Operation | Throughput (ops/s) | Δ throughput | "
            "Latency (µs) | Δ latency |"
        )
        lines.append("|---|--:|--:|--:|--:|")
        for name, cur in new_ops.items():
            old = prev_ops.get(name)
            if old is None:
                lines.append(
                    f"| {name} | {cur['throughput']:,.0f} | _new_ | "
                    f"{cur['latency_us']:.3f} | _new_ |"
                )
                continue
            d_tput = pct(cur["throughput"], old["throughput"])
            d_lat = pct(cur["latency_us"], old["latency_us"])
            lines.append(
                f"| {name} | {cur['throughput']:,.0f} | "
                f"{fmt_delta(d_tput, higher_is_better=True)} | "
                f"{cur['latency_us']:.3f} | "
                f"{fmt_delta(d_lat, higher_is_better=False)} |"
            )
        lines.append("")
        lines.append("Δ throughput: higher is better. Δ latency: lower is better. "
                     "⚠️ marks a regression over 5%.")
    else:
        lines.append("_Baseline — no previous results to compare against._")
        lines.append("")
        lines.append("| Operation | Throughput (ops/s) | Latency (µs) |")
        lines.append("|---|--:|--:|")
        for name, cur in new_ops.items():
            lines.append(
                f"| {name} | {cur['throughput']:,.0f} | {cur['latency_us']:.3f} |"
            )

    lines.append("")
    report = "\n".join(lines)
    with open(args.out, "w") as fh:
        fh.write(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
