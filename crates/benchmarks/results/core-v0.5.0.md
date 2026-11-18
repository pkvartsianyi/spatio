## spatio core v0.5.0 — benchmark results

Platform `aarch64` · dataset 100000 · 2026-11-18T18:47:34.270897+00:00

Compared against **core-v0.4.0**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 184,057 | -0.7% | 5.441 | -11.2% |
| UPDATE | 132,189 | +81.8% | 7.706 | -44.1% |
| GET | 6,431,516 | -5.3% ⚠️ | 0.156 | +4.8% |
| RADIUS | 97,160 | -88.6% ⚠️ | 10.443 | +772.8% ⚠️ |
| KNN | 145,537 | +1.2% | 6.896 | -1.4% |
| DISTANCE | 4,239,402 | -6.1% ⚠️ | 0.244 | +10.0% ⚠️ |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.


RADIUS isn't comparable with v0.4.0. The benchmark used a 0.05 m radius, which matched at most one point, and now uses 5 km. GET and DISTANCE moved by about the run-to-run noise on this machine.
