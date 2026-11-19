import pytest

from spatio import LineString
from spatio import Point
from spatio import Polygon
from spatio import Shape
from spatio import Spatio

SQUARE = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]


@pytest.fixture
def db():
    return Spatio.memory()


def road():
    return LineString([(0.0, 0.0), (0.0, 1.0)])


class TestLineString:
    def test_coords_and_len(self):
        line = road()
        assert line.coords == [(0.0, 0.0), (0.0, 1.0)]
        assert len(line) == 2
        assert "LineString" in repr(line)

    def test_needs_two_vertices(self):
        with pytest.raises(ValueError, match="at least 2"):
            LineString([(0.0, 0.0)])

    def test_length(self):
        assert road().length() == pytest.approx(111_195, rel=1e-3)

    def test_distance_and_is_within(self):
        line = road()
        p = Point(0.001, 0.5)
        assert line.distance_to(p) == pytest.approx(111.2, abs=1.0)
        assert line.is_within(p, 120.0)
        assert not line.is_within(p, 100.0)

    def test_intersections(self):
        cross = LineString([(-0.5, 0.5), (0.5, 0.5)])
        apart = LineString([(1.0, 0.0), (1.0, 1.0)])
        assert road().intersects(cross)
        assert not road().intersects(apart)
        points = road().intersections(cross)
        assert len(points) == 1
        assert points[0].lon == pytest.approx(0.0, abs=1e-9)
        assert points[0].lat == pytest.approx(0.5, abs=1e-3)
        assert road().intersections(apart) == []

    def test_equality(self):
        assert road() == road()
        assert road() != LineString([(0.0, 0.0), (1.0, 1.0)])


class TestShape:
    def test_polygon(self):
        shape = Shape.polygon(Polygon(SQUARE))
        assert shape.kind == "polygon"
        polygon = shape.as_polygon()
        assert polygon is not None
        assert polygon.exterior == SQUARE
        assert shape.center is None
        assert shape.contains(Point(0.5, 0.5))
        assert shape.distance_to(Point(0.5, 0.5)) == 0.0
        assert shape.distance_to(Point(1.001, 0.5)) == pytest.approx(111.2, abs=1.0)
        assert shape.is_within(Point(1.001, 0.5), 200.0)

    def test_circle(self):
        shape = Shape.circle(Point(0.0, 0.0), 500.0)
        assert shape.kind == "circle"
        assert shape.center is not None
        assert (shape.center.lon, shape.center.lat) == (0.0, 0.0)
        assert shape.radius_m == 500.0
        assert shape.line is None
        assert shape.as_polygon() is None
        assert shape.contains(Point(0.001, 0.0))
        assert not shape.contains(Point(0.01, 0.0))
        assert shape.distance_to(Point(0.01, 0.0)) == pytest.approx(612, abs=1.0)
        assert shape.is_within(Point(0.01, 0.0), 700.0)
        assert not shape.is_within(Point(0.01, 0.0), 600.0)

    def test_corridor(self):
        shape = Shape.corridor(road(), 50.0)
        assert shape.kind == "corridor"
        assert shape.line == road()
        assert shape.half_width_m == 50.0
        assert shape.radius_m is None
        assert shape.contains(Point(0.0001, 0.5))
        assert not shape.contains(Point(0.001, 0.5))
        assert shape.distance_to(Point(0.001, 0.5)) == pytest.approx(61.2, abs=1.0)

    def test_repr_and_equality(self):
        assert "circle" in repr(Shape.circle(Point(1.0, 2.0), 10.0))
        assert Shape.circle(Point(1.0, 2.0), 10.0) == Shape.circle(
            Point(1.0, 2.0), 10.0
        )
        assert Shape.circle(Point(1.0, 2.0), 10.0) != Shape.corridor(road(), 10.0)

    @pytest.mark.parametrize(
        ("make", "message"),
        [
            (lambda: Shape.circle(Point(0.0, 0.0), -1.0), "Radius must be positive"),
            (lambda: Shape.circle(Point(0.0, 95.0), 10.0), "Latitude out of range"),
            (lambda: Shape.corridor(road(), -1.0), "half width"),
            (lambda: Shape.corridor(road(), float("nan")), "half width"),
            (
                lambda: Shape.polygon(Polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)])),
                "at least 3 distinct vertices",
            ),
        ],
    )
    def test_invalid_shapes_are_rejected(self, make, message):
        with pytest.raises(ValueError, match=message):
            make()


class TestShapeStorage:
    def test_set_get_delete(self, db):
        circle = Shape.circle(Point(0.0, 0.0), 500.0)
        assert db.get_shape("zones", "depot") is None
        db.set_shape("zones", "depot", circle)
        assert db.get_shape("zones", "depot") == circle
        assert db.get_shape("other", "depot") is None

        db.set_shape("zones", "depot", Shape.circle(Point(0.0, 0.0), 100.0))
        stored = db.get_shape("zones", "depot")
        assert stored is not None
        assert stored.radius_m == 100.0

        assert db.delete_shape("zones", "depot") is True
        assert db.delete_shape("zones", "depot") is False
        assert db.get_shape("zones", "depot") is None

    def test_list_shapes(self, db):
        assert db.list_shapes("zones") == []
        db.set_shape("zones", "a", Shape.circle(Point(0.0, 0.0), 10.0))
        db.set_shape("zones", "b", Shape.corridor(road(), 10.0))
        db.set_shape("zones", "c", Shape.polygon(Polygon(SQUARE)))
        listed = dict(db.list_shapes("zones"))
        assert sorted(listed) == ["a", "b", "c"]
        assert [listed[k].kind for k in ("a", "b", "c")] == [
            "circle",
            "corridor",
            "polygon",
        ]

    def test_shapes_containing(self, db):
        db.set_shape("zones", "square", Shape.polygon(Polygon(SQUARE)))
        db.set_shape("zones", "circle", Shape.circle(Point(0.5, 0.5), 1000.0))
        db.set_shape("zones", "road", Shape.corridor(road(), 50.0))

        assert sorted(db.shapes_containing("zones", Point(0.5, 0.5))) == [
            "circle",
            "square",
        ]
        assert sorted(db.shapes_containing("zones", Point(-0.0001, 0.5))) == ["road"]
        assert db.shapes_containing("zones", Point(5.0, 5.0)) == []
        assert len(db.shapes_containing("zones", Point(0.5, 0.5), limit=1)) == 1
        assert db.shapes_containing("missing", Point(0.5, 0.5)) == []

    def test_shapes_within_nearest_first(self, db):
        db.set_shape("zones", "near", Shape.circle(Point(0.0, 0.0), 100.0))
        db.set_shape("zones", "far", Shape.circle(Point(0.02, 0.0), 100.0))
        db.set_shape("zones", "away", Shape.circle(Point(5.0, 5.0), 100.0))

        results = db.shapes_within("zones", Point(0.0, 0.0), 5000.0)
        assert [r[0] for r in results] == ["near", "far"]
        assert results[0][1] == 0.0
        assert results[1][1] == pytest.approx(2124, abs=2.0)

        assert db.shapes_within("zones", Point(0.0, 0.0), 5000.0, limit=1) == [
            ("near", 0.0)
        ]
        assert db.shapes_within("zones", Point(0.0, 0.0), 0.0) == [("near", 0.0)]

    def test_invalid_arguments(self, db):
        shape = Shape.circle(Point(0.0, 0.0), 10.0)
        with pytest.raises(ValueError, match="namespace must not be empty"):
            db.set_shape("", "a", shape)
        with pytest.raises(ValueError, match="shape_id must not be empty"):
            db.set_shape("zones", "", shape)
        with pytest.raises(ValueError, match="Latitude out of range"):
            db.shapes_containing("zones", Point(0.0, 100.0))
        with pytest.raises(ValueError, match="Distance must be finite"):
            db.shapes_within("zones", Point(0.0, 0.0), -1.0)
        with pytest.raises(ValueError, match="Distance must be finite"):
            db.shapes_within("zones", Point(0.0, 0.0), float("inf"))
        with pytest.raises(TypeError):
            db.set_shape("zones", "a", Polygon(SQUARE))

    def test_closed_database(self, db):
        db.close()
        with pytest.raises(RuntimeError, match="Database is closed"):
            db.set_shape("zones", "a", Shape.circle(Point(0.0, 0.0), 10.0))
        with pytest.raises(RuntimeError, match="Database is closed"):
            db.get_shape("zones", "a")
        with pytest.raises(RuntimeError, match="Database is closed"):
            db.shapes_within("zones", Point(0.0, 0.0), 10.0)

    def test_shapes_survive_reopen(self, tmp_path):
        path = str(tmp_path / "shapes.db")
        circle = Shape.circle(Point(13.4, 52.5), 250.0)
        corridor = Shape.corridor(LineString([(13.0, 52.0), (13.5, 52.5)]), 30.0)
        polygon = Shape.polygon(Polygon(SQUARE))

        with Spatio.open(path) as db:
            db.set_shape("zones", "circle", circle)
            db.set_shape("zones", "corridor", corridor)
            db.set_shape("zones", "polygon", polygon)
            db.set_shape("zones", "gone", circle)
            assert db.delete_shape("zones", "gone")

        with Spatio.open(path) as db:
            assert dict(db.list_shapes("zones")) == {
                "circle": circle,
                "corridor": corridor,
                "polygon": polygon,
            }
            assert db.shapes_containing("zones", Point(13.4, 52.5)) == ["circle"]
