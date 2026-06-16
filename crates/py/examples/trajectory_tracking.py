"""Trajectory tracking with Spatio: record a path and query it over time."""

import spatio


def main() -> None:
    db = spatio.Spatio.memory()

    # A vehicle moving east, one sample every 60 seconds starting at t=1000.
    base_lon, lat = -74.0060, 40.7128
    trajectory = [
        spatio.TemporalPoint(
            spatio.Point(base_lon + i * 0.01, lat),
            timestamp=1000.0 + i * 60.0,
        )
        for i in range(10)
    ]
    db.insert_trajectory("fleet", "truck-1", trajectory)

    # Query the full recorded path (newest first), bounded by a time range.
    path = db.query_trajectory("fleet", "truck-1", 0.0, 10_000.0, limit=100)
    print(f"Recorded {len(path)} points for truck-1")
    for point, _metadata, timestamp in path[:3]:
        print(f"  t={timestamp:.0f}s  ({point.lon:.4f}, {point.lat:.4f})")

    # Query just a sub-window of the trajectory.
    window = db.query_trajectory("fleet", "truck-1", 1000.0, 1180.0, limit=100)
    print(f"\nPoints in the first 3 minutes: {len(window)}")

    # The current (latest) location is available via get().
    point, _metadata, timestamp = db.get("fleet", "truck-1")
    print(f"Current position: ({point.lon:.4f}, {point.lat:.4f}) at t={timestamp:.0f}s")

    db.close()


if __name__ == "__main__":
    main()
