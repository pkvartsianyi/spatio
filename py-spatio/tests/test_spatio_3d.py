
import spatio


class TestPoint3D:
    """Test 3D Point class functionality"""

    def test_valid_point_creation(self):
        """Test creating valid 3D points"""
        point = spatio.Point(40.7128, -74.0060, 8848.86)
        assert point.lat == 40.7128
        assert point.lon == -74.0060
        assert point.alt == 8848.86

    def test_point_repr(self):
        """Test 3D point string representation"""
        point = spatio.Point(40.7128, -74.0060, 8848.86)
        assert "Point(lat=40.7128, lon=-74.006, alt=8848.86)" in str(point)

    def test_point_operations(self):
        """Test geographic point operations with 3D points"""
        db = spatio.Spatio.memory()

        # Insert points
        everest = spatio.Point(27.9881, 86.9250, 8848.86)
        db.insert_point("mountains", everest, b"Mount Everest")

        # Find nearby points
        nearby = db.query_within_radius("mountains", everest, 1000.0, 1)
        assert len(nearby) == 1

        # Each result should be (point, value, distance)
        for point, value, distance in nearby:
            assert isinstance(point, spatio.Point)
            assert isinstance(value, bytes)
            assert isinstance(distance, float)
            assert point.alt == 8848.86
