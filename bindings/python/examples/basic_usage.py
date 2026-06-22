"""Basic usage of the Spatio spatio-temporal database."""

import spatio


def main() -> None:
    # Open an in-memory database (use Spatio.open(path) to persist).
    db = spatio.Spatio.memory()

    # Upsert object locations. Points are (longitude, latitude[, altitude]).
    cities = {
        "nyc": spatio.Point(-74.0060, 40.7128),
        "philly": spatio.Point(-75.1652, 39.9526),
        "boston": spatio.Point(-71.0589, 42.3601),
    }
    for name, point in cities.items():
        db.upsert("cities", name, point, {"name": name})

    # Fetch a single object: (point, metadata, timestamp).
    point, metadata, _timestamp = db.get("cities", "nyc")
    print(f"nyc -> ({point.lon:.4f}, {point.lat:.4f}) {metadata}")

    # Radius query around NYC (meters). Returns (id, point, metadata, distance).
    nearby = db.query_radius("cities", cities["nyc"], 200_000.0, limit=10)
    print(f"\nWithin 200 km of NYC ({len(nearby)} found):")
    for object_id, _point, _metadata, distance in nearby:
        print(f"  {object_id}: {distance / 1000:.1f} km")

    # K-nearest-neighbours.
    print("\n2 nearest to NYC:")
    for object_id, _point, _metadata, distance in db.knn("cities", cities["nyc"], k=2):
        print(f"  {object_id}: {distance / 1000:.1f} km")

    # Bounding-box query (min_x, min_y, max_x, max_y).
    in_box = db.query_bbox("cities", -76.0, 39.0, -70.0, 43.0, limit=10)
    print(f"\nInside bounding box: {[oid for oid, *_ in in_box]}")

    print(f"\nStats: {db.stats()['hot_state_objects']} objects tracked")
    db.close()


if __name__ == "__main__":
    main()
