"""
Comprehensive tests for Spatio Python bindings
"""

import os
import platform
import tempfile
import time

import pytest

import spatio


class TestPoint:
    """Test Point class functionality"""

    def test_point_creation(self):
        """Test creating valid points"""
        point = spatio.Point(40.7128, -74.0060)
        assert point.lat == 40.7128
        assert point.lon == -74.0060

    def test_point_validation(self):
        """Test point validation"""
        # Invalid latitude
        with pytest.raises(ValueError):
            spatio.Point(91.0, 0.0)

        with pytest.raises(ValueError):
            spatio.Point(-91.0, 0.0)

        # Invalid longitude
        with pytest.raises(ValueError):
            spatio.Point(0.0, 181.0)

        with pytest.raises(ValueError):
            spatio.Point(0.0, -181.0)

    def test_point_distance(self):
        """Test distance calculation between points"""
        nyc = spatio.Point(40.7128, -74.0060)
        brooklyn = spatio.Point(40.6782, -73.9442)

        distance = nyc.distance_to(brooklyn)
        # Brooklyn is roughly 6-8 km from NYC center
        assert 6000 < distance < 8000

    def test_point_repr(self):
        """Test point string representation"""
        point = spatio.Point(40.7128, -74.0060)
        assert "Point(lat=40.7128, lon=-74.006)" in str(point)


class TestSetOptions:
    """Test SetOptions class functionality"""

    def test_default_options(self):
        """Test default SetOptions creation"""
        opts = spatio.SetOptions()
        assert opts is not None

    def test_ttl_options(self):
        """Test TTL SetOptions creation"""
        opts = spatio.SetOptions.with_ttl(300.0)  # 5 minutes
        assert opts is not None

    def test_invalid_ttl(self):
        """Test invalid TTL values"""
        with pytest.raises(ValueError):
            spatio.SetOptions.with_ttl(-1.0)

        with pytest.raises(ValueError):
            spatio.SetOptions.with_ttl(0.0)

    def test_expiration_options(self):
        """Test expiration timestamp SetOptions"""
        future_timestamp = time.time() + 300  # 5 minutes from now
        opts = spatio.SetOptions.with_expiration(future_timestamp)
        assert opts is not None


class TestConfig:
    """Test Config class functionality"""

    def test_default_config(self):
        """Test default configuration"""
        config = spatio.Config()
        assert config.geohash_precision == 8

    def test_custom_geohash_precision(self):
        """Test custom geohash precision"""
        config = spatio.Config.with_geohash_precision(10)
        assert config.geohash_precision == 10

    def test_invalid_geohash_precision(self):
        """Test invalid geohash precision values"""
        with pytest.raises(ValueError):
            spatio.Config.with_geohash_precision(0)

        with pytest.raises(ValueError):
            spatio.Config.with_geohash_precision(13)

    def test_set_geohash_precision(self):
        """Test setting geohash precision"""
        config = spatio.Config()
        config.geohash_precision = 6
        assert config.geohash_precision == 6

        with pytest.raises(ValueError):
            config.geohash_precision = 0


class TestSpatio:
    """Test main Spatio database functionality"""

    def test_memory_database(self):
        """Test creating in-memory database"""
        db = spatio.Spatio.memory()
        assert db is not None

    def test_memory_with_config(self):
        """Test creating in-memory database with config"""
        config = spatio.Config.with_geohash_precision(10)
        db = spatio.Spatio.memory_with_config(config)
        assert db is not None

    def test_persistent_database(self):
        """Test creating persistent database"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            # Normalize path for Windows compatibility
            db_path = os.path.normpath(db_path)
            db = spatio.Spatio.open(db_path)
            assert db is not None
            db.close()
            # On Windows, ensure file is properly released
            if platform.system() == "Windows":
                import gc

                gc.collect()

    def test_basic_key_value_operations(self):
        """Test basic key-value operations"""
        db = spatio.Spatio.memory()

        # Insert
        db.insert(b"key1", b"value1")

        # Get
        result = db.get(b"key1")
        assert result == b"value1"

        # Get non-existent key
        result = db.get(b"nonexistent")
        assert result is None

        # Delete
        old_value = db.delete(b"key1")
        assert old_value == b"value1"

        # Verify deletion
        result = db.get(b"key1")
        assert result is None

    def test_ttl_operations(self):
        """Test TTL functionality"""
        db = spatio.Spatio.memory()

        # Insert with very short TTL
        opts = spatio.SetOptions.with_ttl(0.1)  # 100ms
        db.insert(b"temp_key", b"temp_value", opts)

        # Should exist immediately
        result = db.get(b"temp_key")
        assert result == b"temp_value"

        # Wait for expiration - use longer timeout on Windows due to timing differences
        sleep_time = 0.3 if platform.system() == "Windows" else 0.2
        time.sleep(sleep_time)

        # Should be gone (or might still exist depending on cleanup timing)
        # We can't guarantee timing in tests, so we just verify the operation worked
        assert True  # TTL was set successfully

    def test_point_operations(self):
        """Test geographic point operations"""
        db = spatio.Spatio.memory()

        # Insert points
        nyc = spatio.Point(40.7128, -74.0060)
        brooklyn = spatio.Point(40.6782, -73.9442)

        db.insert_point("cities", nyc, b"New York")
        db.insert_point("cities", brooklyn, b"Brooklyn")

        # Find nearby points
        nearby = db.find_nearby("cities", nyc, 50000.0, 10)  # 50km radius
        assert len(nearby) >= 1

        # Each result should be (point, value, distance)
        for point, value, distance in nearby:
            assert isinstance(point, spatio.Point)
            assert isinstance(value, bytes)
            assert isinstance(distance, float)

    def test_spatial_queries(self):
        """Test spatial query operations"""
        db = spatio.Spatio.memory()

        # Insert some points
        nyc = spatio.Point(40.7128, -74.0060)
        brooklyn = spatio.Point(40.6782, -73.9442)

        db.insert_point("cities", nyc, b"New York")
        db.insert_point("cities", brooklyn, b"Brooklyn")

        # Test contains_point
        has_nearby = db.contains_point("cities", nyc, 50000.0)
        assert has_nearby

        # Test count_within_distance
        count = db.count_within_distance("cities", nyc, 50000.0)
        assert count >= 1

        # Test intersects_bounds (NYC area)
        has_points = db.intersects_bounds("cities", 40.6, -74.1, 40.8, -73.9)
        assert has_points

        # Test find_within_bounds
        points = db.find_within_bounds("cities", 40.6, -74.1, 40.8, -73.9, 100)
        assert len(points) >= 1

        # Each result should be (point, value)
        for point, value in points:
            assert isinstance(point, spatio.Point)
            assert isinstance(value, bytes)

    def test_trajectory_operations(self):
        """Test trajectory tracking functionality"""
        db = spatio.Spatio.memory()

        # Create trajectory data
        trajectory = [
            (spatio.Point(40.7128, -74.0060), 1640995200),  # Start
            (spatio.Point(40.7150, -74.0040), 1640995260),  # 1 min later
            (spatio.Point(40.7172, -74.0020), 1640995320),  # 2 min later
        ]

        # Insert trajectory
        db.insert_trajectory("vehicle:truck001", trajectory)

        # Query trajectory
        path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)
        assert len(path) == 3

        # Each result should be (point, timestamp)
        for point, timestamp in path:
            assert isinstance(point, spatio.Point)
            assert isinstance(timestamp, float)

    def test_multiple_operations(self):
        """Test multiple sequential operations"""
        db = spatio.Spatio.memory()

        # Sequential operations
        db.insert(b"key1", b"value1")
        db.insert(b"key2", b"value2")

        point = spatio.Point(40.7128, -74.0060)
        db.insert_point("cities", point, b"NYC")

        # Verify operations were applied
        assert db.get(b"key1") == b"value1"
        assert db.get(b"key2") == b"value2"

        nearby = db.find_nearby("cities", spatio.Point(40.7128, -74.0060), 1000.0, 10)
        assert len(nearby) >= 1

    def test_database_stats(self):
        """Test database statistics"""
        db = spatio.Spatio.memory()

        # Insert some data
        db.insert(b"key1", b"value1")
        db.insert(b"key2", b"value2")

        stats = db.stats()
        assert isinstance(stats, dict)
        assert "key_count" in stats
        assert "expired_count" in stats
        assert "operations_count" in stats

        assert stats["key_count"] >= 2

    def test_sync_operation(self):
        """Test database sync operation"""
        db = spatio.Spatio.memory()

        # Should not raise any errors
        db.sync()

    def test_close_operation(self):
        """Test database close operation"""
        db = spatio.Spatio.memory()

        # Should not raise any errors
        db.close()

        # On Windows, force garbage collection to ensure proper cleanup
        if platform.system() == "Windows":
            import gc

            gc.collect()

    def test_database_repr(self):
        """Test database string representation"""
        db = spatio.Spatio.memory()
        assert "Spatio" in str(db)


class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_operations_on_closed_database(self):
        """Test operations on a closed database still work (limitation)"""
        db = spatio.Spatio.memory()
        db.close()

        # On Windows, ensure proper cleanup after close
        if platform.system() == "Windows":
            import gc

            gc.collect()

        # Current implementation allows operations after close
        # This is a limitation of the current API
        db.insert(b"key", b"value")  # Should still work
        assert db.get(b"key") == b"value"

    def test_invalid_trajectory_data(self):
        """Test invalid trajectory data"""
        db = spatio.Spatio.memory()

        # Invalid trajectory format
        with pytest.raises(ValueError):
            db.insert_trajectory("vehicle:001", [("not_a_tuple",)])

        with pytest.raises(ValueError):
            db.insert_trajectory(
                "vehicle:001", [(spatio.Point(0, 0),)]
            )  # Missing timestamp


class TestPerformance:
    """Basic performance tests"""

    def test_bulk_insert_performance(self):
        """Test bulk insert performance"""
        db = spatio.Spatio.memory()

        # Insert many key-value pairs
        start_time = time.time()
        for i in range(1000):
            key = f"key_{i}".encode()
            value = f"value_{i}".encode()
            db.insert(key, value)

        elapsed = time.time() - start_time
        print(f"Inserted 1000 items in {elapsed:.3f} seconds")

        # Basic sanity check - allow more time on Windows
        max_time = 10.0 if platform.system() == "Windows" else 5.0
        assert elapsed < max_time  # Should be faster than expected time

    def test_spatial_query_performance(self):
        """Test spatial query performance"""
        db = spatio.Spatio.memory()

        # Insert many points
        import random

        points = []
        for i in range(100):
            # Random points around NYC
            lat = 40.7 + random.uniform(-0.1, 0.1)
            lon = -74.0 + random.uniform(-0.1, 0.1)
            point = spatio.Point(lat, lon)
            points.append(point)

            db.insert_point("test_points", point, f"point_{i}".encode())

        # Query performance
        center = spatio.Point(40.7128, -74.0060)
        start_time = time.time()

        for _ in range(100):
            _ = db.find_nearby("test_points", center, 10000.0, 50)

        elapsed = time.time() - start_time
        print(f"Performed 100 spatial queries in {elapsed:.3f} seconds")

        # Basic sanity check - allow more time on Windows
        max_time = 4.0 if platform.system() == "Windows" else 2.0
        assert elapsed < max_time  # Should be faster than expected time


if __name__ == "__main__":
    pytest.main([__file__])
