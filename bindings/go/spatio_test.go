package spatio_test

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/twpayne/go-geom"

	spatio "github.com/pkvartsianyi/spatio/bindings/go"
)

func point(lon, lat float64) *geom.Point {
	return geom.NewPointFlat(geom.XY, []float64{lon, lat})
}

// openTestDB opens an in-memory DB or skips if the native library is missing
// (e.g. `go test` run without building the lib first).
func openTestDB(t *testing.T) *spatio.DB {
	t.Helper()
	db, err := spatio.OpenMemory()
	if err != nil {
		t.Skipf("native library unavailable (build it with `just go-build-lib` or set SPATIO_LIB_PATH): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpsertGetDelete(t *testing.T) {
	db := openTestDB(t)

	nyc := point(-74.0060, 40.7128)
	if err := db.Upsert("cities", "nyc", nyc, map[string]any{"population": 8_000_000}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	loc, err := db.Get("cities", "nyc")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loc == nil {
		t.Fatal("expected nyc, got nil")
	}
	if loc.ObjectID != "nyc" {
		t.Errorf("object id = %q, want nyc", loc.ObjectID)
	}
	meta, err := loc.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if got := meta["population"]; got != float64(8_000_000) {
		t.Errorf("population = %v, want 8000000", got)
	}
	if dx := math.Abs(loc.Point.X() - -74.0060); dx > 1e-9 {
		t.Errorf("x = %v, want -74.0060", loc.Point.X())
	}
	if loc.Point.SRID() != 4326 {
		t.Errorf("SRID = %d, want 4326", loc.Point.SRID())
	}

	// Missing object -> nil, no error.
	missing, err := db.Get("cities", "atlantis")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Error("expected nil for missing object")
	}

	if err := db.Delete("cities", "nyc"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	gone, _ := db.Get("cities", "nyc")
	if gone != nil {
		t.Error("expected nil after delete")
	}
}

func TestRadiusKNNNearAndBBox(t *testing.T) {
	db := openTestDB(t)
	cities := map[string]*geom.Point{
		"nyc":    point(-74.0060, 40.7128),
		"newark": point(-74.1724, 40.7357),
		"sf":     point(-122.4194, 37.7749),
	}
	for id, p := range cities {
		if err := db.Upsert("cities", id, p, map[string]any{"name": id}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}

	// 50km around NYC: nyc + newark, not sf.
	near, err := db.QueryRadius("cities", cities["nyc"], 50_000, 10)
	if err != nil {
		t.Fatalf("query_radius: %v", err)
	}
	if len(near) != 2 {
		t.Fatalf("radius result = %d, want 2", len(near))
	}
	for _, n := range near {
		if n.Distance < 0 {
			t.Errorf("negative distance for %s", n.ObjectID)
		}
	}

	// KNN k=1 from NYC is NYC itself (distance ~0).
	knn, err := db.KNN("cities", cities["nyc"], 1)
	if err != nil {
		t.Fatalf("knn: %v", err)
	}
	if len(knn) != 1 || knn[0].ObjectID != "nyc" {
		t.Fatalf("knn = %+v, want [nyc]", knn)
	}

	// QueryNear by object id.
	byObj, err := db.QueryNear("cities", "nyc", 50_000, 10)
	if err != nil {
		t.Fatalf("query_near: %v", err)
	}
	if len(byObj) != 2 {
		t.Errorf("query_near = %d, want 2", len(byObj))
	}

	// BBox covering the US east coast finds nyc + newark.
	box, err := db.QueryBBox("cities", -75, 40, -73, 41, 10)
	if err != nil {
		t.Fatalf("query_bbox: %v", err)
	}
	if len(box) != 2 {
		t.Errorf("bbox = %d, want 2", len(box))
	}
}

func TestPolygonHullAndBounds(t *testing.T) {
	db := openTestDB(t)
	pts := map[string]*geom.Point{
		"a": point(0, 0),
		"b": point(10, 0),
		"c": point(10, 10),
		"d": point(0, 10),
		"e": point(5, 5),
	}
	for id, p := range pts {
		if err := db.Upsert("grid", id, p, nil); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}

	// Polygon covering the lower-left quadrant.
	poly := geom.NewPolygonFlat(geom.XY, []float64{-1, -1, 6, -1, 6, 6, -1, 6, -1, -1}, []int{10})
	inside, err := db.QueryPolygon("grid", poly, 10)
	if err != nil {
		t.Fatalf("query_polygon: %v", err)
	}
	if len(inside) == 0 {
		t.Error("expected points inside polygon")
	}

	hull, err := db.ConvexHull("grid")
	if err != nil {
		t.Fatalf("convex_hull: %v", err)
	}
	if hull == nil {
		t.Fatal("expected a hull polygon")
	}

	bounds, err := db.BoundingBox("grid")
	if err != nil {
		t.Fatalf("bounding_box: %v", err)
	}
	if bounds == nil {
		t.Fatal("expected bounds")
	}
	if bounds.Min(0) != 0 || bounds.Max(0) != 10 {
		t.Errorf("bounds x = [%v,%v], want [0,10]", bounds.Min(0), bounds.Max(0))
	}

	// Empty namespace -> nil bounds, no error.
	empty, err := db.BoundingBox("nonexistent")
	if err != nil {
		t.Fatalf("bounding_box empty: %v", err)
	}
	if empty != nil {
		t.Error("expected nil bounds for empty namespace")
	}
}

func TestTrajectory(t *testing.T) {
	db := openTestDB(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// XYM line: M carries the unix-seconds timestamp.
	flat := []float64{
		-74.00, 40.70, float64(base.Unix()),
		-74.01, 40.71, float64(base.Add(time.Minute).Unix()),
		-74.02, 40.72, float64(base.Add(2 * time.Minute).Unix()),
	}
	line := geom.NewLineStringFlat(geom.XYM, flat)
	if err := db.InsertTrajectory("fleet", "truck-1", line); err != nil {
		t.Fatalf("insert_trajectory: %v", err)
	}

	start := float64(base.Add(-time.Minute).Unix())
	end := float64(base.Add(10 * time.Minute).Unix())
	traj, err := db.QueryTrajectory("fleet", "truck-1", start, end, 100)
	if err != nil {
		t.Fatalf("query_trajectory: %v", err)
	}
	if len(traj) != 3 {
		t.Fatalf("trajectory points = %d, want 3", len(traj))
	}
	if traj[0].Point == nil {
		t.Error("trajectory point missing geometry")
	}
}

func TestDistance(t *testing.T) {
	db := openTestDB(t)
	_ = db.Upsert("cities", "nyc", point(-74.0060, 40.7128), nil)
	_ = db.Upsert("cities", "sf", point(-122.4194, 37.7749), nil)

	d, err := db.DistanceBetween("cities", "nyc", "sf", spatio.Haversine)
	if err != nil {
		t.Fatalf("distance_between: %v", err)
	}
	// NYC-SF great-circle distance is ~4100 km.
	if d < 4_000_000 || d > 4_200_000 {
		t.Errorf("distance = %v m, want ~4.1e6", d)
	}

	dt, err := db.DistanceTo("cities", "nyc", point(-74.0060, 40.7128), spatio.Haversine)
	if err != nil {
		t.Fatalf("distance_to: %v", err)
	}
	if dt > 1 {
		t.Errorf("distance to same point = %v, want ~0", dt)
	}

	// Missing object -> ErrObjectNotFound.
	_, err = db.DistanceBetween("cities", "nyc", "ghost", spatio.Haversine)
	if !errors.Is(err, spatio.ErrObjectNotFound) {
		t.Errorf("err = %v, want ErrObjectNotFound", err)
	}
}

func TestErrorsAfterClose(t *testing.T) {
	db, err := spatio.OpenMemory()
	if err != nil {
		t.Skipf("native library unavailable: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := db.Get("ns", "id"); !errors.Is(err, spatio.ErrClosed) {
		t.Errorf("err = %v, want ErrClosed", err)
	}
}

func TestStats(t *testing.T) {
	db := openTestDB(t)
	_ = db.Upsert("ns", "a", point(1, 1), nil)
	_ = db.Upsert("ns", "b", point(2, 2), nil)

	stats, err := db.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.HotStateObjects != 2 {
		t.Errorf("hot objects = %d, want 2", stats.HotStateObjects)
	}
}

// BenchmarkQueryRadius exercises the read path (FFI call + binary decode) over a
// dense namespace, returning ~hundreds of results per call. Metadata is left
// undecoded (lazy), which is the common hot-path case.
func BenchmarkQueryRadius(b *testing.B) {
	db, err := spatio.OpenMemory()
	if err != nil {
		b.Skipf("native library unavailable: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10_000; i++ {
		lon := -74.0 + float64(i%100)*0.001
		lat := 40.0 + float64(i/100)*0.001
		if err := db.Upsert("f", fmt.Sprintf("o%d", i), point(lon, lat), map[string]any{"i": i}); err != nil {
			b.Fatal(err)
		}
	}
	center := point(-74.0+0.05, 40.0+0.05)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := db.QueryRadius("f", center, 10_000, 1_000)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) == 0 {
			b.Fatal("expected results")
		}
	}
}
