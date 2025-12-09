
import pytest
import spatio
from spatio import Point, Spatio, TemporalPoint
import time

@pytest.fixture
def db():
    return Spatio.memory()

def test_insert_trajectory(db):
    namespace = "traj_test"
    points = [
        TemporalPoint(Point(0,0,0), time.time()),
        TemporalPoint(Point(1,1,0), time.time() + 1),
        TemporalPoint(Point(2,2,0), time.time() + 2),
    ]
    
    db.insert_trajectory(namespace, "drone1", points)
    
    # Verify current location is the last point
    locs = db.query_radius(namespace, Point(2,2,0), 0.1)
    assert len(locs) == 1
    assert locs[0][0] == "drone1"

def test_bbox_queries(db):
    namespace = "bbox_test"
    db.upsert(namespace, "p1", Point(0,0,0))
    db.upsert(namespace, "p2", Point(10,10,0))
    
    # 2D BBox
    results = db.query_bbox(namespace, -1, -1, 1, 1)
    assert len(results) == 1
    assert results[0][0] == "p1"
    
    # 3D BBox
    # p1 is at z=0. 
    # Query z mainly
    results = db.query_within_bbox_3d(namespace, -1, -1, -1, 1, 1, 1)
    assert len(results) == 1
    assert results[0][0] == "p1"

def test_cylinder_query(db):
    namespace = "cyl_test"
    # Ground point
    db.upsert(namespace, "g1", Point(0,0,0))
    # Air point (same lat/lon)
    db.upsert(namespace, "a1", Point(0,0,1000))
    
    # Query cylinder: Radius 100m, but only z 500-1500
    results = db.query_within_cylinder(namespace, Point(0,0,0), 500, 1500, 100)
    
    assert len(results) == 1
    assert results[0][0] == "a1"

def test_relative_queries(db):
    namespace = "rel_test"
    db.upsert(namespace, "center", Point(0,0,0))
    db.upsert(namespace, "target", Point(0,0,10))
    
    # BBox near object
    results = db.query_bbox_near_object(namespace, "center", 20, 20) # huge box
    assert len(results) >= 1
    
    # Cylinder near object
    # Near "center", lookup up z=500..1500 (should be empty)
    results = db.query_cylinder_near_object(namespace, "center", 500, 1500, 10)
    assert len(results) == 0
