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


def main():
    print("=== Spatio-Py Basic Usage Example ===\n")

    # 1. Create an in-memory database
    print("1. Creating in-memory database...")
    db = spatio.Spatio.memory()
    print("[OK] Database created")

    # 2. Basic key-value operations
    print("\n2. Basic key-value operations...")

    # Insert some data
    db.insert(b"user:123", b"John Doe")
    db.insert(b"user:456", b"Jane Smith")
    db.insert(b"config:timeout", b"30")

    # Retrieve data
    user = db.get(b"user:123")
    print(f"[OK] Retrieved user: {user.decode()}")

    # Check for non-existent key
    missing = db.get(b"user:999")
    print(f"[OK] Missing user: {missing}")

    # 3. Geographic point operations
    print("\n3. Geographic point operations...")

    # Create some famous city points
    nyc = spatio.Point(40.7128, -74.0060)
    london = spatio.Point(51.5074, -0.1278)
    tokyo = spatio.Point(35.6762, 139.6503)
    paris = spatio.Point(48.8566, 2.3522)

    print(f"[OK] Created points: NYC {nyc}, London {london}")

    # Insert points with spatial indexing
    db.insert_point("cities", nyc, b"New York City")
    db.insert_point("cities", london, b"London")
    db.insert_point("cities", tokyo, b"Tokyo")
    db.insert_point("cities", paris, b"Paris")

    print("[OK] Inserted 4 cities with spatial indexing")

    # 4. Spatial queries
    print("\n4. Spatial queries...")

    # Find cities near NYC (within 6000km to include European cities)
    nearby = db.find_nearby("cities", nyc, 6000000.0, 10)
    print(f"[OK] Found {len(nearby)} cities within 6000km of NYC:")

    for _point, city_name, distance in nearby:
        distance_km = distance / 1000
        print(f"  - {city_name.decode()}: {distance_km:.0f}km away")

    # Count points within a smaller radius
    local_count = db.count_within_distance("cities", nyc, 1000000.0)  # 1000km
    print(f"[OK] Cities within 1000km of NYC: {local_count}")

    # Check if any points exist in a region
    has_european_cities = db.intersects_bounds("cities", 40.0, -10.0, 60.0, 10.0)
    print(f"[OK] European cities exist: {has_european_cities}")

    # Find all points in a bounding box (Europe)
    european_cities = db.find_within_bounds("cities", 40.0, -10.0, 60.0, 10.0, 10)
    print(f"[OK] Found {len(european_cities)} European cities:")
    for point, city_name in european_cities:
        print(f"  - {city_name.decode()} at ({point.lat:.2f}, {point.lon:.2f})")

    # 5. TTL (Time-To-Live) functionality
    print("\n5. TTL functionality...")

    # Insert data with TTL
    ttl_options = spatio.SetOptions.with_ttl(2.0)  # 2 seconds
    db.insert(b"session:temp", b"temporary_data", ttl_options)

    # Verify it exists
    temp_data = db.get(b"session:temp")
    print(f"[OK] Temporary data: {temp_data.decode() if temp_data else 'None'}")

    print("[WAIT] Waiting 3 seconds for TTL expiration...")
    time.sleep(3)

    # Check if expired (note: manual cleanup might be needed)
    expired_data = db.get(b"session:temp")
    print(
        f"[OK] After TTL: {expired_data.decode() if expired_data else 'Expired/None'}"
    )

    # 6. Multiple sequential operations
    print("\n6. Multiple sequential operations...")

    # Insert multiple values sequentially
    db.insert(b"batch:key1", b"value1")
    db.insert(b"batch:key2", b"value2")

    # Insert a point
    sf = spatio.Point(37.7749, -122.4194)
    db.insert_point("cities", sf, b"San Francisco")

    print("[OK] Sequential operations completed")

    # Verify operations
    batch_value = db.get(b"batch:key1")
    sf_cities = db.find_nearby("cities", spatio.Point(37.7749, -122.4194), 10000.0, 5)
    print(f"[OK] Sequential value: {batch_value.decode()}")
    print(f"[OK] SF area cities: {len(sf_cities)}")

    # 7. Database statistics
    print("\n7. Database statistics...")
    stats = db.stats()
    print("[OK] Database stats:")
    print(f"  - Key count: {stats['key_count']}")
    print(f"  - Operations count: {stats['operations_count']}")
    print(f"  - Expired count: {stats['expired_count']}")

    # 8. Distance calculations
    print("\n8. Distance calculations...")
    distance_ny_london = nyc.distance_to(london)
    distance_london_paris = london.distance_to(paris)

    print(f"[OK] NYC to London: {distance_ny_london / 1000:.0f}km")
    print(f"[OK] London to Paris: {distance_london_paris / 1000:.0f}km")

    print("\n=== Example completed successfully! ===")


if __name__ == "__main__":
    main()
