package spatio

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/twpayne/go-geom"
)

// DB is a handle to an embedded Spatio database. It is safe for concurrent use:
// the underlying engine handles its own locking, and Close is synchronized
// against in-flight operations. A DB must be closed with Close.
type DB struct {
	// mu guards handle so Close cannot free the native database while another
	// goroutine is calling into it. Operations take RLock (and run
	// concurrently); Close takes the write lock and so waits for them to finish.
	mu     sync.RWMutex
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

// call runs a native function under the read lock with strs converted to C
// strings, and decodes its status.
func (db *DB) call(strs []string, f func(h uintptr, c []cString, errOut unsafe.Pointer) int32) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.handle == 0 {
		return ErrClosed
	}
	c, err := cStrings(strs...)
	if err != nil {
		return err
	}
	var errOut unsafe.Pointer
	code := f(db.handle, c, unsafe.Pointer(&errOut))
	keep(c...)
	return decode(code, errOut)
}

// query is call for functions that return a binary result buffer.
func (db *DB) query(limit int, strs []string, f func(h uintptr, c []cString, outPtr, outLen, errOut unsafe.Pointer) int32) ([]byte, error) {
	if limit < 0 {
		return nil, fmt.Errorf("%w: limit must not be negative", ErrInvalidInput)
	}
	var ptr unsafe.Pointer
	var n uintptr
	err := db.call(strs, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return f(h, c, unsafe.Pointer(&ptr), unsafe.Pointer(&n), errOut)
	})
	if err != nil {
		return nil, err
	}
	return takeBuffer(ptr, n), nil
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
		c, err := cStrings(path)
		if err != nil {
			return nil, err
		}
		code = fnOpen(c[0].ptr(), cfgC.ptr(), unsafe.Pointer(&handle), unsafe.Pointer(&errOut))
		keep(c[0], cfgC)
	}
	if err := decode(code, errOut); err != nil {
		return nil, err
	}
	return &DB{handle: handle}, nil
}

// Close flushes buffered writes and releases the database. It blocks until any
// in-flight operations finish, and is safe to call more than once. The DB must
// not be used afterwards (further calls return ErrClosed). The native handle
// is released even if the final flush fails.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.handle == 0 {
		return nil
	}
	var errOut unsafe.Pointer
	code := fnClose(db.handle, unsafe.Pointer(&errOut))
	db.handle = 0
	return decode(code, errOut)
}

// Upsert inserts or updates an object's current location and metadata.
// Metadata may be any JSON-encodable value; nil stores none.
func (db *DB) Upsert(namespace, objectID string, point *geom.Point, metadata any, opts ...WriteOption) error {
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
	metaC := optCString(metaJSON)
	optsC := optCString(optsJSON)
	err = db.call([]string{namespace, objectID}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnUpsert(h, c[0].ptr(), c[1].ptr(), x, y, z, metaC.ptr(), optsC.ptr(), errOut)
	})
	keep(metaC, optsC)
	return err
}

// Delete removes an object.
func (db *DB) Delete(namespace, objectID string) error {
	return db.call([]string{namespace, objectID}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnDelete(h, c[0].ptr(), c[1].ptr(), errOut)
	})
}

// InsertTrajectory appends a sequence of timestamped positions for an object.
// The line's layout must carry an M ordinate holding unix-seconds timestamps
// (geom.XYM). Trajectories are stored in 2D, so a non-zero Z is rejected.
func (db *DB) InsertTrajectory(namespace, objectID string, line *geom.LineString) error {
	if line == nil {
		return fmt.Errorf("%w: line is nil", ErrInvalidInput)
	}
	mi := line.Layout().MIndex()
	if mi == -1 {
		return fmt.Errorf("%w: trajectory line needs an M ordinate for timestamps (use geom.XYM)", ErrInvalidInput)
	}
	zi := line.Layout().ZIndex()
	coords := line.Coords()
	traj := make([]trajIn, len(coords))
	for i, c := range coords {
		if zi != -1 && c[zi] != 0 {
			return fmt.Errorf("%w: trajectories are 2D; z must be 0", ErrInvalidInput)
		}
		traj[i] = trajIn{X: c[0], Y: c[1], T: c[mi]}
	}
	payload, err := json.Marshal(traj)
	if err != nil {
		return fmt.Errorf("spatio: encoding trajectory: %w", err)
	}
	return db.call([]string{namespace, objectID, string(payload)}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnInsertTrajectory(h, c[0].ptr(), c[1].ptr(), c[2].ptr(), errOut)
	})
}

type trajIn struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	T float64 `json:"t"`
}

// Get returns an object's current location, or nil if it does not exist.
func (db *DB) Get(namespace, objectID string) (*Location, error) {
	buf, err := db.query(0, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnGet(h, c[0].ptr(), c[1].ptr(), p, n, e)
	})
	if err != nil {
		return nil, err
	}
	return decodeLocationOne(buf, namespace), nil
}

// Stats returns a snapshot of database counters.
func (db *DB) Stats() (*Stats, error) {
	var arr [7]uint64
	err := db.call(nil, func(h uintptr, _ []cString, errOut unsafe.Pointer) int32 {
		return fnStats(h, unsafe.Pointer(&arr[0]), errOut)
	})
	if err != nil {
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

func neighbors(buf []byte, err error, namespace string) ([]Neighbor, error) {
	if err != nil {
		return nil, err
	}
	return decodeNeighbors(buf, namespace), nil
}

func locations(buf []byte, err error, namespace string) ([]Location, error) {
	if err != nil {
		return nil, err
	}
	return decodeLocations(buf, namespace), nil
}

// QueryRadius returns objects within radius meters of center, with distances.
func (db *DB) QueryRadius(namespace string, center *geom.Point, radius float64, limit int) ([]Neighbor, error) {
	x, y, z, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	buf, err := db.query(limit, []string{namespace}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryRadius(h, c[0].ptr(), x, y, z, radius, limit, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// QueryNear returns objects within radius meters of another object.
func (db *DB) QueryNear(namespace, objectID string, radius float64, limit int) ([]Neighbor, error) {
	buf, err := db.query(limit, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryNear(h, c[0].ptr(), c[1].ptr(), radius, limit, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// KNN returns the k nearest neighbors of a point.
func (db *DB) KNN(namespace string, center *geom.Point, k int) ([]Neighbor, error) {
	x, y, z, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	buf, err := db.query(k, []string{namespace}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnKNN(h, c[0].ptr(), x, y, z, k, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// KNNNearObject returns the k nearest neighbors of another object.
func (db *DB) KNNNearObject(namespace, objectID string, k int) ([]Neighbor, error) {
	buf, err := db.query(k, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnKNNNearObject(h, c[0].ptr(), c[1].ptr(), k, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// QueryBBox returns objects within a 2D bounding box.
func (db *DB) QueryBBox(namespace string, minX, minY, maxX, maxY float64, limit int) ([]Location, error) {
	buf, err := db.query(limit, []string{namespace}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryBBox(h, c[0].ptr(), minX, minY, maxX, maxY, limit, p, n, e)
	})
	return locations(buf, err, namespace)
}

// QueryWithinCylinder returns objects within a vertical cylinder, with distances.
func (db *DB) QueryWithinCylinder(namespace string, center *geom.Point, minZ, maxZ, radius float64, limit int) ([]Neighbor, error) {
	x, y, _, err := pointXYZ(center)
	if err != nil {
		return nil, err
	}
	buf, err := db.query(limit, []string{namespace}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryCylinder(h, c[0].ptr(), x, y, minZ, maxZ, radius, limit, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// QueryWithinBBox3D returns objects within a 3D bounding box.
func (db *DB) QueryWithinBBox3D(namespace string, minX, minY, minZ, maxX, maxY, maxZ float64, limit int) ([]Location, error) {
	buf, err := db.query(limit, []string{namespace}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryBBox3D(h, c[0].ptr(), minX, minY, minZ, maxX, maxY, maxZ, limit, p, n, e)
	})
	return locations(buf, err, namespace)
}

// QueryBBoxNearObject returns objects within a width×height box centered on an object.
func (db *DB) QueryBBoxNearObject(namespace, objectID string, width, height float64, limit int) ([]Location, error) {
	buf, err := db.query(limit, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryBBoxNear(h, c[0].ptr(), c[1].ptr(), width, height, limit, p, n, e)
	})
	return locations(buf, err, namespace)
}

// QueryCylinderNearObject returns objects within a cylinder centered on an object.
func (db *DB) QueryCylinderNearObject(namespace, objectID string, minZ, maxZ, radius float64, limit int) ([]Neighbor, error) {
	buf, err := db.query(limit, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryCylinderNear(h, c[0].ptr(), c[1].ptr(), minZ, maxZ, radius, limit, p, n, e)
	})
	return neighbors(buf, err, namespace)
}

// QueryBBox3DNearObject returns objects within a width×height×depth box centered on an object.
func (db *DB) QueryBBox3DNearObject(namespace, objectID string, width, height, depth float64, limit int) ([]Location, error) {
	buf, err := db.query(limit, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryBBox3DNear(h, c[0].ptr(), c[1].ptr(), width, height, depth, limit, p, n, e)
	})
	return locations(buf, err, namespace)
}

// QueryPolygon returns objects whose location falls within polygon.
func (db *DB) QueryPolygon(namespace string, polygon *geom.Polygon, limit int) ([]Location, error) {
	geoJSON, err := polygonToGeoJSON(polygon)
	if err != nil {
		return nil, err
	}
	buf, err := db.query(limit, []string{namespace, geoJSON}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryPolygon(h, c[0].ptr(), c[1].ptr(), limit, p, n, e)
	})
	return locations(buf, err, namespace)
}

// QueryTrajectory returns historical samples for an object between start and end.
func (db *DB) QueryTrajectory(namespace, objectID string, start, end float64, limit int) ([]TrajectoryPoint, error) {
	buf, err := db.query(limit, []string{namespace, objectID}, func(h uintptr, c []cString, p, n, e unsafe.Pointer) int32 {
		return fnQueryTrajectory(h, c[0].ptr(), c[1].ptr(), start, end, limit, p, n, e)
	})
	if err != nil {
		return nil, err
	}
	return decodeTrajectory(buf), nil
}

// DistanceBetween returns the distance (meters) between two objects under
// metric. It returns ErrObjectNotFound if either object is missing.
func (db *DB) DistanceBetween(namespace, id1, id2 string, metric DistanceMetric) (float64, error) {
	var dist float64
	var found bool
	err := db.call([]string{namespace, id1, id2, string(metric)}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnDistanceBetween(h, c[0].ptr(), c[1].ptr(), c[2].ptr(), c[3].ptr(),
			unsafe.Pointer(&dist), unsafe.Pointer(&found), errOut)
	})
	if err != nil {
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
	x, y, _, err := pointXYZ(point)
	if err != nil {
		return 0, err
	}
	var dist float64
	var found bool
	err = db.call([]string{namespace, objectID, string(metric)}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnDistanceTo(h, c[0].ptr(), c[1].ptr(), x, y, c[2].ptr(),
			unsafe.Pointer(&dist), unsafe.Pointer(&found), errOut)
	})
	if err != nil {
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
	var outGeo unsafe.Pointer
	err := db.call([]string{namespace}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnConvexHull(h, c[0].ptr(), unsafe.Pointer(&outGeo), errOut)
	})
	if err != nil {
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
	var minX, minY, maxX, maxY float64
	var found bool
	err := db.call([]string{namespace}, func(h uintptr, c []cString, errOut unsafe.Pointer) int32 {
		return fnBoundingBox(h, c[0].ptr(),
			unsafe.Pointer(&minX), unsafe.Pointer(&minY), unsafe.Pointer(&maxX), unsafe.Pointer(&maxY),
			unsafe.Pointer(&found), errOut)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return geom.NewBounds(geom.XY).Set(minX, minY, maxX, maxY), nil
}

// metadataJSON marshals optional metadata, returning nil for nil.
func metadataJSON(m any) (*string, error) {
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
