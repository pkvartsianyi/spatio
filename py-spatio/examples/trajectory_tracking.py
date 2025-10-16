#!/usr/bin/env python3
"""
Trajectory tracking example for Spatio-Py

This example demonstrates trajectory functionality including:
- Recording vehicle movements over time
- Querying trajectory data for specific time ranges
- Analyzing movement patterns
- Real-time trajectory updates
"""

import math
import random
import time

import spatio


def generate_realistic_trajectory(start_point, num_points=20, time_interval=60):
    """
    Generate a realistic trajectory with some randomness

    Args:
        start_point: Starting Point
        num_points: Number of trajectory points to generate
        time_interval: Time between points in seconds

    Returns:
        List of (Point, timestamp) tuples
    """
    trajectory = []
    current_lat = start_point.lat
    current_lon = start_point.lon
    current_time = int(time.time())

    # Simulate movement with small random variations
    for i in range(num_points):
        # Add some random movement (simulate realistic vehicle movement)
        lat_delta = random.uniform(-0.001, 0.001)  # ~100m variation
        lon_delta = random.uniform(-0.001, 0.001)

        # Add a slight trend (simulate moving in a general direction)
        trend_lat = 0.0002 * i  # Moving north
        trend_lon = 0.0003 * i  # Moving east

        current_lat += lat_delta + trend_lat
        current_lon += lon_delta + trend_lon

        point = spatio.Point(current_lat, current_lon)
        timestamp = current_time + (i * time_interval)

        trajectory.append((point, timestamp))

    return trajectory


def simulate_delivery_route():
    """Simulate a delivery truck route through a city"""
    # Starting point (warehouse)
    warehouse = spatio.Point(40.7128, -74.0060)  # NYC

    # Delivery stops
    stops = [
        spatio.Point(40.7505, -73.9934),  # Times Square
        spatio.Point(40.7614, -73.9776),  # Central Park
        spatio.Point(40.7282, -73.7949),  # Queens
        spatio.Point(40.6892, -74.0445),  # Brooklyn
        spatio.Point(40.7128, -74.0060),  # Back to warehouse
    ]

    trajectory = []
    current_time = int(time.time()) - 3600  # Start 1 hour ago

    # Add warehouse start
    trajectory.append((warehouse, current_time))
    current_time += 300  # 5 minutes to get ready

    # Add route between stops
    for i, stop in enumerate(stops):
        # Simulate travel time based on distance
        prev_stop = warehouse if i == 0 else stops[i - 1]

        distance = prev_stop.distance_to(stop)
        travel_time = max(
            300, int(distance / 20)
        )  # Minimum 5 minutes, ~20m/s average speed

        # Add intermediate points during travel
        num_intermediate = max(2, int(travel_time / 180))  # Point every ~3 minutes

        for j in range(num_intermediate):
            # Linear interpolation between points
            ratio = (j + 1) / (num_intermediate + 1)
            interp_lat = prev_stop.lat + ratio * (stop.lat - prev_stop.lat)
            interp_lon = prev_stop.lon + ratio * (stop.lon - prev_stop.lon)

            # Add some GPS noise
            interp_lat += random.uniform(-0.0001, 0.0001)
            interp_lon += random.uniform(-0.0001, 0.0001)

            interp_point = spatio.Point(interp_lat, interp_lon)
            interp_time = current_time + int(
                (j + 1) * travel_time / (num_intermediate + 1)
            )

            trajectory.append((interp_point, interp_time))

        # Add the actual stop
        trajectory.append((stop, current_time + travel_time))
        current_time += travel_time + 600  # 10 minutes at each stop

    return trajectory


def main():
    print("=== Spatio-Py Trajectory Tracking Example ===\n")

    # Create database
    print("1. Creating database...")
    db = spatio.Spatio.memory()
    print("[OK] Database created")

    # 2. Generate and store multiple vehicle trajectories
    print("\n2. Generating vehicle trajectories...")

    vehicles = [
        ("truck_001", spatio.Point(40.7128, -74.0060)),  # NYC
        ("truck_002", spatio.Point(40.7505, -73.9934)),  # Times Square
        ("car_001", spatio.Point(40.6782, -73.9442)),  # Brooklyn
    ]

    all_trajectories = {}

    for vehicle_id, start_point in vehicles:
        print(f"  Generating trajectory for {vehicle_id}...")

        if vehicle_id == "truck_001":
            # Use the detailed delivery route for truck_001
            trajectory = simulate_delivery_route()
        else:
            # Generate random trajectory for others
            trajectory = generate_realistic_trajectory(
                start_point, 15, 180
            )  # Every 3 minutes

        # Store trajectory in database
        db.insert_trajectory(vehicle_id, trajectory)
        all_trajectories[vehicle_id] = trajectory

        print(f"    [OK] Stored {len(trajectory)} points for {vehicle_id}")

    # 3. Query trajectory data
    print("\n3. Querying trajectory data...")

    # Query recent trajectory for truck_001
    current_time = int(time.time())
    start_time = current_time - 7200  # Last 2 hours

    truck_path = db.query_trajectory("truck_001", start_time, current_time)
    print(f"[OK] Retrieved {len(truck_path)} points for truck_001 in last 2 hours")

    if truck_path:
        first_point, first_time = truck_path[0]
        last_point, last_time = truck_path[-1]

        print(
            f"  First point: ({first_point.lat:.4f}, {first_point.lon:.4f}) at {first_time}"
        )
        print(
            f"  Last point: ({last_point.lat:.4f}, {last_point.lon:.4f}) at {last_time}"
        )

        # Calculate total distance traveled
        total_distance = 0
        for i in range(1, len(truck_path)):
            prev_point, _ = truck_path[i - 1]
            curr_point, _ = truck_path[i]
            total_distance += prev_point.distance_to(curr_point)

        print(f"  Total distance: {total_distance / 1000:.2f} km")

    # 4. Analyze movement patterns
    print("\n4. Analyzing movement patterns...")

    for vehicle_id in ["truck_001", "car_001"]:
        trajectory = all_trajectories[vehicle_id]

        if len(trajectory) < 2:
            continue

        print(f"\n  Analysis for {vehicle_id}:")

        # Calculate average speed
        total_distance = 0
        total_time = 0

        for i in range(1, len(trajectory)):
            prev_point, prev_time = trajectory[i - 1]
            curr_point, curr_time = trajectory[i]

            distance = prev_point.distance_to(curr_point)
            time_diff = curr_time - prev_time

            total_distance += distance
            total_time += time_diff

        if total_time > 0:
            avg_speed_ms = total_distance / total_time
            avg_speed_kmh = avg_speed_ms * 3.6

            print(f"    Average speed: {avg_speed_kmh:.1f} km/h")
            print(f"    Total distance: {total_distance / 1000:.2f} km")
            print(f"    Duration: {total_time / 60:.1f} minutes")

        # Find the point farthest from start
        start_point, _ = trajectory[0]
        max_distance = 0

        for point, _timestamp in trajectory:
            distance = start_point.distance_to(point)
            if distance > max_distance:
                max_distance = distance

        print(f"    Farthest from start: {max_distance / 1000:.2f} km")

    # 5. Real-time trajectory updates
    print("\n5. Simulating real-time updates...")

    # Add new points to an existing trajectory
    vehicle_id = "truck_002"
    last_trajectory = all_trajectories[vehicle_id]

    if last_trajectory:
        last_point, last_time = last_trajectory[-1]

        # Simulate 3 new GPS updates
        new_points = []
        current_lat = last_point.lat
        current_lon = last_point.lon
        current_time = last_time

        for _ in range(3):
            # Simulate movement
            current_lat += random.uniform(-0.0005, 0.0005)
            current_lon += random.uniform(-0.0005, 0.0005)
            current_time += 120  # 2 minutes later

            new_point = spatio.Point(current_lat, current_lon)
            new_points.append((new_point, current_time))

        print(f"  Adding {len(new_points)} new points to {vehicle_id}...")

        # In a real application, you might append to existing trajectory
        # For demo, we'll create a new trajectory with the additional points
        extended_trajectory = last_trajectory + new_points
        db.insert_trajectory(f"{vehicle_id}_extended", extended_trajectory)

        print(f"  [OK] Extended trajectory now has {len(extended_trajectory)} points")

    # 6. Spatial queries on trajectory data
    print("\n6. Spatial queries on trajectory data...")

    # Find all vehicles that were near Times Square
    times_square = spatio.Point(40.7505, -73.9934)

    vehicles_near_times_square = []

    for vehicle_id in all_trajectories:
        trajectory = all_trajectories[vehicle_id]

        for point, timestamp in trajectory:
            distance = times_square.distance_to(point)
            if distance < 500:  # Within 500 meters
                vehicles_near_times_square.append(
                    (vehicle_id, point, timestamp, distance)
                )
                break  # Just find the first occurrence

    print(f"[OK] Found {len(vehicles_near_times_square)} vehicles near Times Square:")
    for vehicle_id, _point, timestamp, distance in vehicles_near_times_square:
        print(f"  - {vehicle_id}: {distance:.0f}m away at timestamp {timestamp}")

    # 7. Time-based analysis
    print("\n7. Time-based analysis...")

    # Find vehicles active during a specific time window
    analysis_start = current_time - 5400  # 1.5 hours ago
    analysis_end = current_time - 1800  # 30 minutes ago

    active_vehicles = []

    for vehicle_id in all_trajectories:
        trajectory = all_trajectories[vehicle_id]

        points_in_window = [
            (point, timestamp)
            for point, timestamp in trajectory
            if analysis_start <= timestamp <= analysis_end
        ]

        if points_in_window:
            active_vehicles.append((vehicle_id, len(points_in_window)))

    print(f"[OK] Vehicles active between {analysis_start} and {analysis_end}:")
    for vehicle_id, point_count in active_vehicles:
        print(f"  - {vehicle_id}: {point_count} data points")

    # 8. Database statistics
    print("\n8. Database statistics...")
    stats = db.stats()
    print("[OK] Final database stats:")
    print(f"  - Total keys: {stats['key_count']}")
    print(f"  - Operations: {stats['operations_count']}")

    print("\n=== Trajectory example completed successfully! ===")


if __name__ == "__main__":
    main()
