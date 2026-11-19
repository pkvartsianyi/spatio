## spatio core v0.6.0 — benchmark results

Spatio can now store shapes next to points: polygons, circles and corridors (a line plus a width in meters). `shapes_containing` and `shapes_within` find the shapes at or near a point, and shapes are saved to `<db>.shapes` next to the log. `LineString` adds point-to-line distance and line intersection on great circles. See #100.

Go users need the matching library: the C ABI wire format is now v3.

Platform `aarch64` · dataset 100000 · 2026-11-19T21:53:45.862444+00:00

Compared against **core-v0.5.0**.

| Operation | Throughput (ops/s) | Δ throughput | Latency (µs) | Δ latency |
|---|--:|--:|--:|--:|
| UPSERT | 208,367 | +13.2% | 4.800 | -11.8% |
| UPDATE | 150,542 | +13.9% | 6.644 | -13.8% |
| GET | 7,462,417 | +16.0% | 0.135 | -13.3% |
| RADIUS | 114,535 | +17.9% | 8.742 | -16.3% |
| KNN | 161,910 | +11.2% | 6.182 | -10.4% |
| DISTANCE | 4,342,832 | +2.4% | 0.239 | -2.1% |

Δ throughput: higher is better. Δ latency: lower is better. ⚠️ marks a regression over 5%.

None of the measured operations changed in this release. Shapes have their own index and lock. The gains over v0.5.0 are run-to-run variation on this machine, not a speedup.
