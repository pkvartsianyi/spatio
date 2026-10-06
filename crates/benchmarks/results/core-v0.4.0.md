## spatio core v0.4.0 — benchmark results

Platform `aarch64` · dataset 100000 · 2026-10-06T09:35:57.616043+00:00

Compared against **core-v0.3.9**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 185,324 | -13.9% ⚠️ | 6.130 | +31.9% ⚠️ |
| UPDATE | 72,730 | -8.8% ⚠️ | 13.775 | +9.9% ⚠️ |
| GET | 6,788,256 | -16.2% ⚠️ | 0.149 | +20.1% ⚠️ |
| RADIUS | 853,055 | -17.6% ⚠️ | 1.197 | +22.5% ⚠️ |
| KNN | 143,810 | -55.4% ⚠️ | 6.994 | +125.7% ⚠️ |
| DISTANCE | 4,514,527 | -4.6% | 0.222 | +5.2% ⚠️ |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.

