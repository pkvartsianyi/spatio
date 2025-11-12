#!/usr/bin/env python3
"""
Basic usage example for Spatio-Py

This example demonstrates the core functionality of Spatio including:
- Creating databases
- Basic key-value operations
- Geographic point operations
- Spatial queries
- TTL functionality
"""

import time

import spatio


def _create_and_test_database():
    print("1. Creating in-memory database...")
    db = spatio.Spatio.memory()
    print("[OK] Database created")

    print("\n2. Basic key-value operations...")
    db.insert(b"user:123", b"John Doe")
    db.insert(b"user:456", b"Jane Smith")
    db.insert(b"config:timeout", b"30")

    user = db.get(b"user:123")
    print(f"[OK] Retrieved user: {user.decode()}")

    missing = db.get(b"user:999")
    print(f"[OK] Missing user: {missing}")
    return db


def _demonstrate_geographic_operations(db):
    print("\n3. Geographic point operations...")
    nyc = spatio.Point(-74.0060, 40.7128)
    london = spatio.Point(-0.1278, 51.5074)
    tokyo = spatio.Point(139.6503, 35.6762)
    paris = spatio.Point(2.3522, 48.8566)

    print(f"[OK] Created points: NYC {nyc}, London {london}")

    db.insert_point("cities", nyc, b"New York City")
    db.insert_point("cities", london, b"London")
    db.insert_point("cities", tokyo, b"Tokyo")
    db.insert_point("cities", paris, b"Paris")

    print("[OK] Inserted 4 cities with spatial indexing")

    print("\n4. Spatial queries...")
    nearby = db.query_within_radius("cities", nyc, 6000000.0, 10)
    print(f"[OK] Found {len(nearby)} cities within 6000km of NYC:")

    for _point, city_name, distance in nearby:
        distance_km = distance / 1000
        print(f"  - {city_name.decode()}: {distance_km:.0f}km away")

    local_count = db.count_within_radius("cities", nyc, 1000000.0)
    print(f"[OK] Cities within 1000km of NYC: {local_count}")

    has_european_cities = db.intersects_bounds("cities", 40.0, -10.0, 60.0, 10.0)
    print(f"[OK] European cities exist: {has_european_cities}")

    european_cities = db.find_within_bounds("cities", 40.0, -10.0, 60.0, 10.0, 10)
    print(f"[OK] Found {len(european_cities)} European cities:")
    for point, city_name in european_cities:
        print(f"  - {city_name.decode()} at ({point.lat:.2f}, {point.lon:.2f})")
    return nyc, london, paris


def _demonstrate_ttl_functionality(db):
    print("\n5. TTL (Time-To-Live) functionality...")
    ttl_options = spatio.SetOptions.with_ttl(2.0)
    db.insert(b"session:temp", b"temporary_data", ttl_options)

    temp_data = db.get(b"session:temp")
    print(f"[OK] Temporary data: {temp_data.decode() if temp_data else 'None'}")

    print("[WAIT] Waiting 3 seconds for TTL expiration...")
    time.sleep(3)

    expired_data = db.get(b"session:temp")
    print(
        f"[OK] After TTL: {expired_data.decode() if expired_data else 'Expired/None'}"
    )


def _demonstrate_sequential_operations(db):
    print("\n6. Multiple sequential operations...")
    db.insert(b"batch:key1", b"value1")
    db.insert(b"batch:key2", b"value2")

    sf = spatio.Point(37.7749, -122.4194)
    db.insert_point("cities", sf, b"San Francisco")

    print("[OK] Sequential operations completed")

    batch_value = db.get(b"batch:key1")
    sf_cities = db.query_within_radius(
        "cities", spatio.Point(37.7749, -122.4194), 10000.0, 5
    )
    print(f"[OK] Sequential value: {batch_value.decode()}")
    print(f"[OK] SF area cities: {len(sf_cities)}")


def _demonstrate_database_statistics(db):
    print("\n7. Database statistics...")
    stats = db.stats()
    print("[OK] Database stats:")
    print(f"  - Key count: {stats['key_count']}")
    print(f"  - Operations count: {stats['operations_count']}")
    print(f"  - Expired count: {stats['expired_count']}")


def _demonstrate_distance_calculations(nyc, london, paris):
    print("\n8. Distance calculations...")
    distance_ny_london = nyc.distance_to(london)
    distance_london_paris = london.distance_to(paris)

    print(f"[OK] NYC to London: {distance_ny_london / 1000:.0f}km")
    print(f"[OK] London to Paris: {distance_london_paris / 1000:.0f}km")


def _demonstrate_3d_point_operations(db):
    print("\n9. 3D Point Operations...")
    everest = spatio.Point(27.9881, 86.9250, 8848.86)
    db.insert_point("mountains", everest, b"Mount Everest")
    results = db.query_within_radius("mountains", everest, 1000.0, 1)
    for point, name, _ in results:
        print(f"[OK] Found 3D point: {name.decode()} at {point}")


def main():
    print("=== Spatio-Py Basic Usage Example ===\n")
    db = _create_and_test_database()
    nyc, london, paris = _demonstrate_geographic_operations(db)
    _demonstrate_ttl_functionality(db)
    _demonstrate_sequential_operations(db)
    _demonstrate_database_statistics(db)
    _demonstrate_distance_calculations(nyc, london, paris)
    _demonstrate_3d_point_operations(db)
    print("\n=== Example completed successfully! ===")
