## spatio core v0.3.9 — benchmark results

Platform `aarch64` · dataset 100000 · 2026-06-22T08:09:45.363904+00:00

Compared against **core-v0.3.8**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 215,173 | +6.1% | 4.648 | -5.9% |
| UPDATE | 79,757 | +2.8% | 12.538 | -2.7% |
| GET | 8,105,065 | +9.8% | 0.124 | -9.1% |
| RADIUS | 1,035,606 | +4.3% | 0.977 | -5.2% |
| KNN | 322,803 | +5.4% | 3.098 | -5.3% |
| DISTANCE | 4,734,268 | +5.2% | 0.211 | -5.8% |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.

