package spatio

import (
	"encoding/json"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/twpayne/go-geom"
)

// DB is a handle to an embedded Spatio database. It is safe for concurrent use:
// the underlying engine handles its own locking. A DB must be closed with Close.
type DB struct {
	handle uintptr
}

// keep prevents the backing buffers of C strings from being collected until
// after the native call that used them.
func keep(cs ...cString) {
	for _, c := range cs {
		runtime.KeepAlive(c.buf)
	}
}

// takeBuffer copies a native result buffer into a Go-owned slice and frees the
// native allocation. Decoders then reference sub-slices of this copy (e.g. lazy
// metadata) without further allocation, and without dangling into freed memory.
func takeBuffer(ptr unsafe.Pointer, n uintptr) []byte {
	if ptr == nil || n == 0 {
		return nil
	}
	buf := make([]byte, int(n))
	copy(buf, unsafe.Slice((*byte)(ptr), int(n)))
	fnBufferFree(ptr, n)
	return buf
}

// OpenMemory creates an in-memory database.
func OpenMemory(opts ...Option) (*DB, error) {
	return open("", true, opts...)
}

// Open opens (or creates) a persistent database at path.
func Open(path string, opts ...Option) (*DB, error) {
	return open(path, false, opts...)
}

func open(path string, inMemory bool, opts ...Option) (*DB, error) {
	if err := ensureLoaded(); err != nil {
		return nil, err
	}
	var cfg openConfig
	for _, o := range opts {
		o(&cfg)
	}
	cfgJSON, err := cfg.json()
	if err != nil {
		return nil, fmt.Errorf("spatio: encoding config: %w", err)
	}
	cfgC := optCString(cfgJSON)

	var handle uintptr
	var errOut unsafe.Pointer
	var code int32
	if inMemory {
		code = fnOpenMemory(cfgC.ptr(), unsafe.Pointer(&handle), unsafe.Pointer(&errOut))
		keep(cfgC)
	} else {
		pathC := newCString(path)
		code = fnOpen(pathC.ptr(), cfgC.ptr(), unsafe.Pointer(&handle), unsafe.Pointer(&errOut))
		keep(pathC, cfgC)
	}
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return &DB{handle: handle}, nil
}

// Close flushes buffered writes and releases the database. The DB must not be
// used afterwards.
func (db *DB) Close() error {
	if db.handle == 0 {
		return nil
	}
	var errOut unsafe.Pointer
	code := fnClose(db.handle, unsafe.Pointer(&errOut))
	db.handle = 0
	return decode(code, errOut)
}

func (db *DB) check() error {
	if db.handle == 0 {
		return ErrClosed
	}
	return nil
}

// Upsert inserts or updates an object's current location and metadata.
func (db *DB) Upsert(namespace, objectID string, point *geom.Point, metadata map[string]any, opts ...WriteOption) error {
	if err := db.check(); err != nil {
		return err
	}
	x, y, z, err := pointXYZ(point)
	if err != nil {
		return err
	}
	metaJSON, err := metadataJSON(metadata)
	if err != nil {
		return fmt.Errorf("spatio: encoding metadata: %w", err)
	}
	var wo writeOptions
	for _, o := range opts {
		o(&wo)
	}
	optsJSON, err := wo.json()
	if err != nil {
		return fmt.Errorf("spatio: encoding write options: %w", err)
	}

	nsC := newCString(namespace)
	idC := newCString(objectID)
	metaC := optCString(metaJSON)
	optsC := optCString(optsJSON)
	var errOut unsafe.Pointer
	code := fnUpsert(db.handle, nsC.ptr(), idC.ptr(), x, y, z, metaC.ptr(), optsC.ptr(), unsafe.Pointer(&errOut))
	keep(nsC, idC, metaC, optsC)
	return decode(code, errOut)
}

// Delete removes an object.
func (db *DB) Delete(namespace, objectID string) error {
	if err := db.check(); err != nil {
		return err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var errOut unsafe.Pointer
	code := fnDelete(db.handle, nsC.ptr(), idC.ptr(), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return decode(code, errOut)
}

// InsertTrajectory appends a sequence of timestamped positions for an object.
// The line's layout must carry an M ordinate holding unix-seconds timestamps
// (geom.XYM or geom.XYZM).
func (db *DB) InsertTrajectory(namespace, objectID string, line *geom.LineString) error {
	if err := db.check(); err != nil {
		return err
	}
	if line == nil {
		return fmt.Errorf("%w: line is nil", ErrInvalidInput)
	}
	mi := line.Layout().MIndex()
	if mi == -1 {
		return fmt.Errorf("%w: trajectory line needs an M ordinate for timestamps (use geom.XYM)", ErrInvalidInput)
	}
	coords := line.Coords()
	traj := make([]trajIn, len(coords))
	for i, c := range coords {
		traj[i] = trajIn{X: c[0], Y: c[1], T: c[mi]}
	}
	payload, err := json.Marshal(traj)
	if err != nil {
		return fmt.Errorf("spatio: encoding trajectory: %w", err)
	}

	nsC := newCString(namespace)
	idC := newCString(objectID)
	trajC := newCString(string(payload))
	var errOut unsafe.Pointer
	code := fnInsertTrajectory(db.handle, nsC.ptr(), idC.ptr(), trajC.ptr(), unsafe.Pointer(&errOut))
	keep(nsC, idC, trajC)
	return decode(code, errOut)
}

type trajIn struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	T float64 `json:"t"`
}

// Get returns an object's current location, or nil if it does not exist.
func (db *DB) Get(namespace, objectID string) (*Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnGet(db.handle, nsC.ptr(), idC.ptr(), unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return decodeLocationOne(takeBuffer(ptr, n), namespace), nil
}

// Stats returns a snapshot of database counters.
func (db *DB) Stats() (*Stats, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	var arr [7]uint64
	var errOut unsafe.Pointer
	code := fnStats(db.handle, unsafe.Pointer(&arr[0]), unsafe.Pointer(&errOut))
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return &Stats{
		ExpiredCount:          arr[0],
		OperationsCount:       arr[1],
		SizeBytes:             arr[2],
		HotStateObjects:       arr[3],
		ColdStateTrajectories: arr[4],
		ColdStateBufferBytes:  arr[5],
		MemoryUsageBytes:      arr[6],
	}, nil
}

// finishNeighbors / finishLocations decode the status + binary buffer for the
// two common result shapes, then free the buffer.
func finishNeighbors(code int32, ptr unsafe.Pointer, n uintptr, errOut unsafe.Pointer, namespace string) ([]Neighbor, error) {
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return decodeNeighbors(takeBuffer(ptr, n), namespace), nil
}

func finishLocations(code int32, ptr unsafe.Pointer, n uintptr, errOut unsafe.Pointer, namespace string) ([]Location, error) {
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return decodeLocations(takeBuffer(ptr, n), namespace), nil
}

// QueryRadius returns objects within radius meters of center, with distances.
func (db *DB) QueryRadius(namespace string, center *geom.Point, radius float64, limit int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	x, y, z, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryRadius(db.handle, nsC.ptr(), x, y, z, radius, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// QueryNear returns objects within radius meters of another object.
func (db *DB) QueryNear(namespace, objectID string, radius float64, limit int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryNear(db.handle, nsC.ptr(), idC.ptr(), radius, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// KNN returns the k nearest neighbors of a point.
func (db *DB) KNN(namespace string, center *geom.Point, k int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	x, y, z, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnKNN(db.handle, nsC.ptr(), x, y, z, k, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// KNNNearObject returns the k nearest neighbors of another object.
func (db *DB) KNNNearObject(namespace, objectID string, k int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnKNNNearObject(db.handle, nsC.ptr(), idC.ptr(), k, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// QueryBBox returns objects within a 2D bounding box.
func (db *DB) QueryBBox(namespace string, minX, minY, maxX, maxY float64, limit int) ([]Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryBBox(db.handle, nsC.ptr(), minX, minY, maxX, maxY, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC)
	return finishLocations(code, ptr, n, errOut, namespace)
}

// QueryWithinCylinder returns objects within a vertical cylinder, with distances.
func (db *DB) QueryWithinCylinder(namespace string, center *geom.Point, minZ, maxZ, radius float64, limit int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	x, y, _, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryCylinder(db.handle, nsC.ptr(), x, y, minZ, maxZ, radius, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// QueryWithinBBox3D returns objects within a 3D bounding box.
func (db *DB) QueryWithinBBox3D(namespace string, minX, minY, minZ, maxX, maxY, maxZ float64, limit int) ([]Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryBBox3D(db.handle, nsC.ptr(), minX, minY, minZ, maxX, maxY, maxZ, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC)
	return finishLocations(code, ptr, n, errOut, namespace)
}

// QueryBBoxNearObject returns objects within a width×height box centered on an object.
func (db *DB) QueryBBoxNearObject(namespace, objectID string, width, height float64, limit int) ([]Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryBBoxNear(db.handle, nsC.ptr(), idC.ptr(), width, height, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return finishLocations(code, ptr, n, errOut, namespace)
}

// QueryCylinderNearObject returns objects within a cylinder centered on an object.
func (db *DB) QueryCylinderNearObject(namespace, objectID string, minZ, maxZ, radius float64, limit int) ([]Neighbor, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryCylinderNear(db.handle, nsC.ptr(), idC.ptr(), minZ, maxZ, radius, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return finishNeighbors(code, ptr, n, errOut, namespace)
}

// QueryBBox3DNearObject returns objects within a width×height×depth box centered on an object.
func (db *DB) QueryBBox3DNearObject(namespace, objectID string, width, height, depth float64, limit int) ([]Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryBBox3DNear(db.handle, nsC.ptr(), idC.ptr(), width, height, depth, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	return finishLocations(code, ptr, n, errOut, namespace)
}

// QueryPolygon returns objects whose location falls within polygon.
func (db *DB) QueryPolygon(namespace string, polygon *geom.Polygon, limit int) ([]Location, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	geoJSON, err := polygonToGeoJSON(polygon)
	if err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	polyC := newCString(geoJSON)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryPolygon(db.handle, nsC.ptr(), polyC.ptr(), limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, polyC)
	return finishLocations(code, ptr, n, errOut, namespace)
}

// QueryTrajectory returns historical samples for an object between start and end.
func (db *DB) QueryTrajectory(namespace, objectID string, start, end float64, limit int) ([]TrajectoryPoint, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	var ptr unsafe.Pointer
	var n uintptr
	var errOut unsafe.Pointer
	code := fnQueryTrajectory(db.handle, nsC.ptr(), idC.ptr(), start, end, limit, unsafe.Pointer(&ptr), unsafe.Pointer(&n), unsafe.Pointer(&errOut))
	keep(nsC, idC)
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return decodeTrajectory(takeBuffer(ptr, n)), nil
}

// DistanceBetween returns the distance (meters) between two objects under
// metric. It returns ErrObjectNotFound if either object is missing.
func (db *DB) DistanceBetween(namespace, id1, id2 string, metric DistanceMetric) (float64, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	nsC := newCString(namespace)
	a := newCString(id1)
	b := newCString(id2)
	m := newCString(string(metric))
	var dist float64
	var found bool
	var errOut unsafe.Pointer
	code := fnDistanceBetween(db.handle, nsC.ptr(), a.ptr(), b.ptr(), m.ptr(),
		unsafe.Pointer(&dist), unsafe.Pointer(&found), unsafe.Pointer(&errOut))
	keep(nsC, a, b, m)
	if err := decode(code, errOut); err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrObjectNotFound
	}
	return dist, nil
}

// DistanceTo returns the distance (meters) from an object to a point under
// metric. It returns ErrObjectNotFound if the object is missing.
func (db *DB) DistanceTo(namespace, objectID string, point *geom.Point, metric DistanceMetric) (float64, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	x, y, _, err := pointXYZ(point)
	if err != nil {
		return 0, err
	}
	nsC := newCString(namespace)
	idC := newCString(objectID)
	m := newCString(string(metric))
	var dist float64
	var found bool
	var errOut unsafe.Pointer
	code := fnDistanceTo(db.handle, nsC.ptr(), idC.ptr(), x, y, m.ptr(),
		unsafe.Pointer(&dist), unsafe.Pointer(&found), unsafe.Pointer(&errOut))
	keep(nsC, idC, m)
	if err := decode(code, errOut); err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrObjectNotFound
	}
	return dist, nil
}

// ConvexHull returns the convex hull of all objects in a namespace, or nil if
// there are fewer than three points.
func (db *DB) ConvexHull(namespace string) (*geom.Polygon, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var outGeo, errOut unsafe.Pointer
	code := fnConvexHull(db.handle, nsC.ptr(), unsafe.Pointer(&outGeo), unsafe.Pointer(&errOut))
	keep(nsC)
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	s := consumeString(outGeo)
	if s == "" {
		return nil, nil
	}
	return geoJSONToPolygon(s)
}

// BoundingBox returns the axis-aligned 2D bounds of all objects in a namespace,
// or nil for an empty namespace.
func (db *DB) BoundingBox(namespace string) (*geom.Bounds, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	nsC := newCString(namespace)
	var minX, minY, maxX, maxY float64
	var found bool
	var errOut unsafe.Pointer
	code := fnBoundingBox(db.handle, nsC.ptr(),
		unsafe.Pointer(&minX), unsafe.Pointer(&minY), unsafe.Pointer(&maxX), unsafe.Pointer(&maxY),
		unsafe.Pointer(&found), unsafe.Pointer(&errOut))
	keep(nsC)
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return geom.NewBounds(geom.XY).Set(minX, minY, maxX, maxY), nil
}

// metadataJSON marshals optional metadata, returning nil for a nil map.
func metadataJSON(m map[string]any) (*string, error) {
	if m == nil {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}
