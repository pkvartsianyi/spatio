"""Simple throughput/latency demo for Spatio using the real API."""

import random
import time

import spatio


def benchmark(label: str, iterations: int, fn) -> None:
    start = time.perf_counter()
    for i in range(iterations):
        fn(i)
    elapsed = time.perf_counter() - start
    per_op_us = elapsed / iterations * 1e6
    print(f"{label:<24} {iterations:>8} ops  {elapsed:6.3f}s  {per_op_us:8.2f} us/op")


def main() -> None:
    random.seed(42)
    db = spatio.Spatio.memory()

    n = 50_000
    points = [
        spatio.Point(random.uniform(-180.0, 180.0), random.uniform(-85.0, 85.0))
        for _ in range(n)
    ]

    benchmark(
        "upsert",
        n,
        lambda i: db.upsert("objects", f"obj-{i}", points[i]),
    )

    center = spatio.Point(0.0, 0.0)
    benchmark(
        "query_radius (1000km)",
        2_000,
        lambda _: db.query_radius("objects", center, 1_000_000.0, limit=50),
    )

    benchmark(
        "knn (k=10)",
        2_000,
        lambda _: db.knn("objects", center, k=10),
    )

    benchmark(
        "get",
        50_000,
        lambda i: db.get("objects", f"obj-{i % n}"),
    )

    print(f"\nFinal object count: {db.stats()['hot_state_objects']}")
    db.close()


if __name__ == "__main__":
    main()
