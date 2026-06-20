## spatio core v0.3.8 — benchmark results

Platform `aarch64` · dataset 100000 · 2026-06-20T12:37:42.725344+00:00

Compared against **core-v0.3.7**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 202,785 | +37.7% | 4.941 | -27.3% |
| UPDATE | 77,565 | +32.4% | 12.893 | -24.7% |
| GET | 7,381,692 | +11.4% | 0.136 | -11.3% |
| RADIUS | 993,014 | +14.8% | 1.030 | -11.7% |
| KNN | 306,286 | -0.6% | 3.272 | +0.8% |
| DISTANCE | 4,498,356 | +4.1% | 0.224 | -3.7% |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.

