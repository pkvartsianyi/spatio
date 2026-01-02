"""
Comprehensive tests for Spatio Python bindings
"""

import gc
import os
import time
import pytest
import spatio

@pytest.fixture
def gc_collect():
    """Fixture for test cleanup"""
    yield
    gc.collect()

class TestPoint:
    """Test Point class functionality"""

    def test_valid_point_creation(self):
        """Test creating valid points"""
        point = spatio.Point(-74.0060, 40.7128)
        assert point.lon == -74.0060
        assert point.lat == 40.7128
        assert point.alt == 0.0

    def test_valid_3d_point_creation(self):
        """Test creating valid 3D points"""
        point = spatio.Point(-74.0060, 40.7128, 100.0)
        assert point.lon == -74.0060
        assert point.lat == 40.7128
        assert point.alt == 100.0

    def test_point_distance(self):
        """Test distance calculation between points"""
        nyc = spatio.Point(-74.0060, 40.7128)
        brooklyn = spatio.Point(-73.9442, 40.6782)

        distance = nyc.distance_to(brooklyn)
        # Brooklyn is roughly 6-8 km from NYC center
        assert 6000 < distance < 8000

    def test_point_repr(self):
        """Test point string representation"""
        point = spatio.Point(-74.0060, 40.7128)
        assert "Point(x=-74.0060, y=40.7128, z=0.0000)" in str(point)

class TestConfig:
    """Test Config class functionality"""

    def test_default_config(self):
        """Test default configuration"""
        config = spatio.Config()
        assert config is not None

class TestSpatio:
    """Test main Spatio database functionality"""

    def test_memory_database(self):
        """Test creating in-memory database"""
        db = spatio.Spatio.memory()
        assert db is not None

    def test_memory_with_config(self):
        """Test creating in-memory database with config"""
        config = spatio.Config()
        db = spatio.Spatio.memory_with_config(config)
        assert isinstance(db, spatio.Spatio)

    def test_persistent_database_from_non_exist_file(self, gc_collect, tmp_path):
        """Test creating persistent database using non-existing file"""
        db_path = os.path.join(tmp_path, "test.db")
        db = spatio.Spatio.open(db_path)
        assert isinstance(db, spatio.Spatio)
        db.close()

    def test_update_location(self):
        """Test updating location"""
        db = spatio.Spatio.memory()
        nyc = spatio.Point(-74.0060, 40.7128)
        
        # Update with metadata
        db.upsert("cities", "nyc", nyc, {"name": "New York"})
        
        # Query to verify
        results = db.query_radius("cities", nyc, 1000.0, 10)
        assert len(results) == 1
        # Expect (obj_id, point, meta, distance)
        obj_id, point, meta, distance = results[0]
        assert obj_id == "nyc"
        assert point.lon == -74.0060
        assert meta == {"name": "New York"}
        assert distance < 1.0  # Should be 0 since it's the same point

    def test_update_location_no_metadata(self):
        """Test updating location without metadata"""
        db = spatio.Spatio.memory()
        nyc = spatio.Point(-74.0060, 40.7128)
        
        db.upsert("cities", "nyc", nyc)
        
        results = db.query_radius("cities", nyc, 1000.0, 10)
        assert len(results) == 1
        # Expect (obj_id, point, meta, distance)
        _, _, meta, _ = results[0]
        assert meta is None

    def test_query_near_object(self):
        """Test querying near another object"""
        db = spatio.Spatio.memory()
        nyc = spatio.Point(-74.0060, 40.7128)
        brooklyn = spatio.Point(-73.9442, 40.6782)
        
        db.upsert("cities", "nyc", nyc, {"name": "NYC"})
        db.upsert("cities", "bk", brooklyn, {"name": "Brooklyn"})
        
        # Query near NYC
        results = db.query_near("cities", "nyc", 10000.0, 10)
        
        # Should find Brooklyn (NYC itself is excluded from near_object query usually, or included? 
        # Rust implementation of query_near_object usually excludes the object itself if it's based on KNN, 
        # but let's check behavior. Actually standard radius query includes it. 
        # Let's assume it finds neighbors.)
        
        # Based on implementation, it does a radius search around the object's position.
        # It likely includes the object itself unless explicitly filtered.
        # Let's verify count.
        assert len(results) >= 1
        
        found_bk = False
        # Expect (obj_id, point, meta, distance)
        for obj_id, _, _, _ in results:
            if obj_id == "bk":
                found_bk = True
        assert found_bk

    def test_trajectory_operations(self):
        """Test trajectory tracking functionality"""
        db = spatio.Spatio.memory()
        
        start_time = time.time()
        
        # Add points to create trajectory
        # Note: In a real test we'd want to control timestamps, but the Python API 
        # currently uses system time for updates.
        
        p1 = spatio.Point(-74.0060, 40.7128)
        db.upsert("vehicle", "truck1", p1, {"step": 1})
        time.sleep(0.01)
        
        p2 = spatio.Point(-74.0040, 40.7150)
        db.upsert("vehicle", "truck1", p2, {"step": 2})
        time.sleep(0.01)
        
        end_time = time.time()
        
        # Query trajectory
        path = db.query_trajectory("vehicle", "truck1", start_time - 1.0, end_time + 1.0, 10)
        assert len(path) == 2
        
        # Verify order (newest first usually)
        # Rust implementation sorts by timestamp descending
        assert path[0][1] == {"step": 2} # metadata
        assert path[1][1] == {"step": 1}

    def test_close_operation(self):
        """Test database close operation"""
        db = spatio.Spatio.memory()
        db.close()
        
        # Operations should fail after close
        with pytest.raises(RuntimeError, match=r"Database is closed"):
            db.upsert("test", "obj", spatio.Point(0, 0))

if __name__ == "__main__":
    pytest.main([__file__])
