## spatio core v0.3.7 — benchmark results

Platform `aarch64` · dataset 100000 · 2026-06-16T08:34:02.281125+00:00

Compared against **core-v0.3.6**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 147,316 | -6.8% ⚠️ | 6.792 | +7.3% ⚠️ |
| UPDATE | 58,592 | -6.5% ⚠️ | 17.128 | +7.3% ⚠️ |
| GET | 6,624,910 | +6.2% | 0.154 | -5.4% |
| RADIUS | 865,291 | +2.0% | 1.167 | -3.2% |
| KNN | 308,129 | +7.5% | 3.246 | -7.8% |
| DISTANCE | 4,322,211 | +28.2% | 0.233 | -37.1% |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.

