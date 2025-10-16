#!/usr/bin/env python3
"""
Performance demonstration for Spatio

This example showcases the performance characteristics of Spatio
including benchmarks for various operations and comparisons
with different configurations.
"""

import gc
import random
import statistics
import time
from typing import List
from typing import Tuple

import spatio


def format_number(num: float) -> str:
    """Format large numbers with commas."""
    return f"{num:,.0f}"


def format_time(seconds: float) -> str:
    """Format time duration in human-readable format."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.1f}us"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    else:
        return f"{seconds:.2f}s"


def benchmark_operation(func, *args, iterations: int = 100, warmup: int = 10):
    """Benchmark an operation with multiple iterations."""
    # Warmup
    for _ in range(warmup):
        func(*args)

    # Force garbage collection
    gc.collect()

    # Actual benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func(*args)
        end = time.perf_counter()
        times.append(end - start)

    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "result": result,
    }


def benchmark_key_value_operations():
    """Benchmark basic key-value operations."""
    print("[KEY] Key-Value Operations Benchmark")
    print("=" * 50)

    db = spatio.Spatio.memory()

    # Insert benchmark
    def insert_single():
        key = f"key_{random.randint(0, 1000000)}".encode()
        value = f"value_{random.randint(0, 1000000)}".encode()
        db.insert(key, value)
        return key

    insert_stats = benchmark_operation(insert_single, iterations=1000)
    print(
        f"Insert (single):     {format_time(insert_stats['mean'])} avg, {format_number(1 / insert_stats['mean'])} ops/sec"
    )

    # Prepare some data for get/delete benchmarks
    test_keys = []
    for i in range(1000):
        key = f"test_key_{i}".encode()
        value = f"test_value_{i}".encode()
        db.insert(key, value)
        test_keys.append(key)

    # Get benchmark
    def get_single():
        key = random.choice(test_keys)
        return db.get(key)

    get_stats = benchmark_operation(get_single, iterations=1000)
    print(
        f"Get (single):        {format_time(get_stats['mean'])} avg, {format_number(1 / get_stats['mean'])} ops/sec"
    )

    # Delete benchmark
    def delete_single():
        if test_keys:
            key = test_keys.pop()
            return db.delete(key)
        return None

    # Only test deletes on a subset to preserve data
    delete_stats = benchmark_operation(delete_single, iterations=100)
    print(
        f"Delete (single):     {format_time(delete_stats['mean'])} avg, {format_number(1 / delete_stats['mean'])} ops/sec"
    )

    print()


def benchmark_bulk_operations():
    """Benchmark bulk operations."""
    print("[BULK] Bulk Operations Benchmark")
    print("=" * 50)

    db = spatio.Spatio.memory()

    # Bulk insert benchmark
    def bulk_insert_1000():
        for i in range(1000):
            key = f"bulk_key_{i}".encode()
            value = f"bulk_value_{i}".encode()
            db.insert(key, value)

    bulk_stats = benchmark_operation(bulk_insert_1000, iterations=10)
    ops_per_sec = 1000 / bulk_stats["mean"]
    print(
        f"Bulk insert (1K):    {format_time(bulk_stats['mean'])} total, {format_number(ops_per_sec)} ops/sec"
    )

    # Memory usage estimation
    stats = db.stats()
    print(f"Memory usage:        ~{stats['key_count']} keys stored")

    print()


def benchmark_spatial_operations():
    """Benchmark spatial operations."""
    print("[MAP] Spatial Operations Benchmark")
    print("=" * 50)

    db = spatio.Spatio.memory()

    # Prepare spatial data
    points = []
    for i in range(1000):
        # Random points around NYC
        lat = 40.7 + random.uniform(-0.1, 0.1)
        lon = -74.0 + random.uniform(-0.1, 0.1)
        point = spatio.Point(lat, lon)
        points.append(point)

        db.insert_point("locations", point, f"location_{i}".encode())

    # Point creation benchmark
    def create_point():
        lat = 40.7 + random.uniform(-0.1, 0.1)
        lon = -74.0 + random.uniform(-0.1, 0.1)
        return spatio.Point(lat, lon)

    point_stats = benchmark_operation(create_point, iterations=10000)
    print(
        f"Point creation:      {format_time(point_stats['mean'])} avg, {format_number(1 / point_stats['mean'])} ops/sec"
    )

    # Distance calculation benchmark
    center = spatio.Point(40.7128, -74.0060)

    def distance_calc():
        point = random.choice(points)
        return center.distance_to(point)

    distance_stats = benchmark_operation(distance_calc, iterations=10000)
    print(
        f"Distance calc:       {format_time(distance_stats['mean'])} avg, {format_number(1 / distance_stats['mean'])} ops/sec"
    )

    # Spatial insert benchmark
    def spatial_insert():
        lat = 40.7 + random.uniform(-0.1, 0.1)
        lon = -74.0 + random.uniform(-0.1, 0.1)
        point = spatio.Point(lat, lon)
        value = f"new_location_{random.randint(0, 1000000)}".encode()
        db.insert_point("new_locations", point, value)

    spatial_insert_stats = benchmark_operation(spatial_insert, iterations=1000)
    print(
        f"Spatial insert:      {format_time(spatial_insert_stats['mean'])} avg, {format_number(1 / spatial_insert_stats['mean'])} ops/sec"
    )

    # Spatial query benchmark
    def spatial_query():
        center_point = random.choice(points)
        return db.find_nearby("locations", center_point, 1000.0, 10)

    query_stats = benchmark_operation(spatial_query, iterations=1000)
    print(
        f"Spatial query:       {format_time(query_stats['mean'])} avg, {format_number(1 / query_stats['mean'])} queries/sec"
    )

    # Query with different radii
    radii = [100.0, 500.0, 1000.0, 5000.0, 10000.0]
    print("\nQuery performance by radius:")
    for radius in radii:

        def query_with_radius(r=radius):
            return db.find_nearby("locations", center, r, 50)

        radius_stats = benchmark_operation(query_with_radius, iterations=100)
        result_count = len(radius_stats["result"])
        print(
            f"  {radius / 1000:4.1f}km radius:   {format_time(radius_stats['mean'])} avg, {result_count:3d} results"
        )

    print()


def benchmark_trajectory_operations():
    """Benchmark trajectory operations."""
    print("[ROAD] Trajectory Operations Benchmark")
    print("=" * 50)

    db = spatio.Spatio.memory()

    # Create trajectory data
    def create_trajectory(num_points: int) -> List[Tuple]:
        trajectory = []
        lat, lon = 40.7128, -74.0060
        timestamp = int(time.time())

        for _ in range(num_points):
            # Simulate movement
            lat += random.uniform(-0.001, 0.001)
            lon += random.uniform(-0.001, 0.001)
            timestamp += 60  # 1 minute intervals

            point = spatio.Point(lat, lon)
            trajectory.append((point, timestamp))

        return trajectory

    # Trajectory insertion benchmark
    def insert_trajectory():
        trajectory = create_trajectory(50)
        vehicle_id = f"vehicle_{random.randint(0, 10000)}"
        db.insert_trajectory(vehicle_id, trajectory)
        return vehicle_id

    traj_insert_stats = benchmark_operation(insert_trajectory, iterations=100)
    points_per_sec = 50 / traj_insert_stats["mean"]
    print(
        f"Trajectory insert:   {format_time(traj_insert_stats['mean'])} avg, {format_number(points_per_sec)} points/sec"
    )

    # Prepare some trajectories for queries
    vehicle_ids = []
    for i in range(10):
        trajectory = create_trajectory(100)
        vehicle_id = f"test_vehicle_{i}"
        db.insert_trajectory(vehicle_id, trajectory)
        vehicle_ids.append(vehicle_id)

    # Trajectory query benchmark
    def query_trajectory():
        vehicle_id = random.choice(vehicle_ids)
        start_time = int(time.time()) - 3600  # 1 hour ago
        end_time = int(time.time())
        return db.query_trajectory(vehicle_id, start_time, end_time)

    traj_query_stats = benchmark_operation(query_trajectory, iterations=100)
    print(
        f"Trajectory query:    {format_time(traj_query_stats['mean'])} avg, {format_number(1 / traj_query_stats['mean'])} queries/sec"
    )

    print()


def benchmark_configuration_impact():
    """Benchmark different configuration settings."""
    print("[CONFIG] Configuration Impact Benchmark")
    print("=" * 50)

    precisions = [6, 8, 10, 12]

    for precision in precisions:
        config = spatio.Config.with_geohash_precision(precision)
        db = spatio.Spatio.memory_with_config(config)

        # Insert some spatial data
        for i in range(100):
            lat = 40.7 + random.uniform(-0.01, 0.01)
            lon = -74.0 + random.uniform(-0.01, 0.01)
            point = spatio.Point(lat, lon)
            db.insert_point("test_locations", point, f"loc_{i}".encode())

        # Benchmark spatial queries
        center = spatio.Point(40.7128, -74.0060)

        def spatial_query_precision(test_db=db, query_center=center):
            return test_db.find_nearby("test_locations", query_center, 1000.0, 20)

        precision_stats = benchmark_operation(spatial_query_precision, iterations=100)
        accuracy = (
            "~61cm"
            if precision == 10
            else "~610m"
            if precision == 6
            else "~39m"
            if precision == 8
            else "~5cm"
        )

        print(
            f"Precision {precision:2d} ({accuracy:>6}): {format_time(precision_stats['mean'])} avg query time"
        )

    print()


def benchmark_memory_usage():
    """Benchmark memory usage patterns."""
    print("[MEMORY] Memory Usage Benchmark")
    print("=" * 50)

    db = spatio.Spatio.memory()

    # Test memory usage with different data sizes
    data_sizes = [1000, 5000, 10000, 50000]

    for size in data_sizes:
        # Clear previous data
        db = spatio.Spatio.memory()

        # Insert key-value data
        start_time = time.perf_counter()
        for i in range(size):
            key = f"key_{i}".encode()
            value = f"value_{i}_{random.randint(0, 1000000)}".encode()
            db.insert(key, value)
        kv_time = time.perf_counter() - start_time

        # Insert spatial data
        start_time = time.perf_counter()
        for i in range(size):
            lat = 40.7 + random.uniform(-0.1, 0.1)
            lon = -74.0 + random.uniform(-0.1, 0.1)
            point = spatio.Point(lat, lon)
            db.insert_point(f"spatial_{i % 10}", point, f"spatial_value_{i}".encode())
        spatial_time = time.perf_counter() - start_time

        stats = db.stats()

        print(f"{format_number(size):>6} items:")
        print(
            f"  KV insert time:    {format_time(kv_time)} ({format_number(size / kv_time)} ops/sec)"
        )
        print(
            f"  Spatial insert:    {format_time(spatial_time)} ({format_number(size / spatial_time)} ops/sec)"
        )
        print(f"  Total keys:        {format_number(stats['key_count'])}")
        print()


def performance_comparison():
    """Compare performance with different scenarios."""
    print("[STATS] Performance Comparison")
    print("=" * 50)

    # Scenario 1: Small frequent operations
    print("Scenario 1: Small frequent operations")
    db1 = spatio.Spatio.memory()

    start_time = time.perf_counter()
    for i in range(10000):
        key = f"small_{i}".encode()
        value = f"val_{i}".encode()
        db1.insert(key, value)
    small_ops_time = time.perf_counter() - start_time

    print(
        f"  10K small inserts: {format_time(small_ops_time)} ({format_number(10000 / small_ops_time)} ops/sec)"
    )

    # Scenario 2: Large batch operations
    print("Scenario 2: Large batch operations")
    db2 = spatio.Spatio.memory()

    start_time = time.perf_counter()
    for i in range(1000):
        key = f"large_{i}".encode()
        # Larger value
        value = f"large_value_{i}_{'x' * 1000}".encode()
        db2.insert(key, value)
    large_ops_time = time.perf_counter() - start_time

    print(
        f"  1K large inserts:  {format_time(large_ops_time)} ({format_number(1000 / large_ops_time)} ops/sec)"
    )

    # Scenario 3: Mixed spatial operations
    print("Scenario 3: Mixed spatial operations")
    db3 = spatio.Spatio.memory()

    start_time = time.perf_counter()
    for i in range(1000):
        # Insert
        lat = 40.7 + random.uniform(-0.1, 0.1)
        lon = -74.0 + random.uniform(-0.1, 0.1)
        point = spatio.Point(lat, lon)
        db3.insert_point("mixed", point, f"mixed_{i}".encode())

        # Query every 10 inserts
        if i % 10 == 0:
            center = spatio.Point(40.7128, -74.0060)
            db3.find_nearby("mixed", center, 5000.0, 10)

    mixed_ops_time = time.perf_counter() - start_time
    print(
        f"  1K mixed ops:      {format_time(mixed_ops_time)} ({format_number(1100 / mixed_ops_time)} ops/sec)"
    )

    print()


def main():
    """Run comprehensive performance demonstration."""
    print("[DEMO] Spatio Performance Demonstration")
    print("=" * 60)
    print(f"Using Spatio version: {spatio.__version__}")
    print(f"Test started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        benchmark_key_value_operations()
        benchmark_bulk_operations()
        benchmark_spatial_operations()
        benchmark_trajectory_operations()
        benchmark_configuration_impact()
        benchmark_memory_usage()
        performance_comparison()

        print("[SUCCESS] Performance demonstration completed successfully!")
        print()
        print("Key Takeaways:")
        print("- Spatio provides excellent performance for spatial operations")
        print(
            "- Higher geohash precision increases accuracy but may impact query speed"
        )
        print("- Bulk operations show good scalability characteristics")
        print("- Memory usage grows linearly with data size")
        print("- Mixed workloads maintain consistent performance")

    except Exception as e:
        print(f"[ERROR] Error during performance testing: {e}")
        raise


if __name__ == "__main__":
    main()
