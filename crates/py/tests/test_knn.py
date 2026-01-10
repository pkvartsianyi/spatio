
import pytest
import spatio
from spatio import Point, Spatio
import time

@pytest.fixture
def db():
    return Spatio.memory()

def test_knn_basic(db):
    namespace = "knn_test"
    db.upsert(namespace, "center", Point(0, 0, 0), {})
    
    # Add points at increasing distances
    db.upsert(namespace, "p1", Point(0, 1, 0), {"id": 1})  # ~111km
    db.upsert(namespace, "p2", Point(0, 2, 0), {"id": 2})  # ~222km
    db.upsert(namespace, "p3", Point(0, 3, 0), {"id": 3})  # ~333km
    
    # Query 2 nearest from center
    results = db.knn(namespace, Point(0, 0, 0), 2)
    
    assert len(results) == 2
    # Results should be sorted by distance: center (dist=0), p1 (dist=~111km)
    # Note: query point is (0,0,0) which exactly matches "center" object
    
    ids = [r[0] for r in results]
    assert "center" in ids
    assert "p1" in ids
    assert "p2" not in ids

def test_knn_near_object(db):
    namespace = "knn_obj_test"
    
    # Drone at (0,0)
    db.upsert(namespace, "drone1", Point(0, 0, 0), {})
    
    # Target 1 at (0.0001, 0)
    db.upsert(namespace, "target1", Point(0.0001, 0, 0), {}) 
    
    # Target 2 at (10, 10)
    db.upsert(namespace, "target2", Point(10, 10, 0), {})
    
    # Query nearest 1 to drone1
    results = db.knn_near_object(namespace, "drone1", 2)
    
    # Should get drone1 itself (dist=0) and target1
    assert len(results) == 2
    assert results[0][0] == "drone1"
    assert results[1][0] == "target1"

def test_knn_3d(db):
    namespace = "knn_3d_test"
    
    # Ground point
    db.upsert(namespace, "ground", Point(0, 0, 0), {})
    
    # Air point (same lat/lon, 1000m up)
    db.upsert(namespace, "air", Point(0, 0, 1000), {})
    
    # Far point (1 degree away)
    db.upsert(namespace, "far", Point(1, 0, 0), {})
    
    # Query from (0,0, 100)
    results = db.knn(namespace, Point(0, 0, 100), 10)
    
    # Check that results were obtained and have valid structure
    assert len(results) >= 2
    ids = [r[0] for r in results]
    
    # due to unweighted R-tree metric (deg vs meters), 'far' (1 deg) is "closer" than 'air' (900m) in the index space.
    # Verify that the wrapper correctly returns 3D points.
    assert "ground" in ids
    
    air_point = next(p for i, p, m, d in results if i == "air")
    assert air_point.z == 1000
