package spatio

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

// srid4326 is the WGS84 spatial reference, applied to all geometries returned
// by the bindings (Spatio works in lon/lat).
const srid4326 = 4326

// DistanceMetric selects how distances are computed.
type DistanceMetric string

const (
	Haversine DistanceMetric = "haversine"
	Geodesic  DistanceMetric = "geodesic"
	Rhumb     DistanceMetric = "rhumb"
	Euclidean DistanceMetric = "euclidean"
)

// Location is an object's current position. Metadata is decoded lazily via the
// Metadata method, so position/distance queries pay nothing for metadata they
// don't read.
type Location struct {
	ObjectID  string
	Namespace string
	Point     *geom.Point
	Timestamp time.Time
	meta      []byte
}

// Metadata decodes the object's metadata as a JSON object, or nil if it has
// none. Use MetadataValue for metadata that is not an object.
func (l *Location) Metadata() (map[string]any, error) {
	return decodeMetadataObject(l.meta)
}

// MetadataValue decodes the object's metadata as any JSON value, or nil.
func (l *Location) MetadataValue() (any, error) {
	return decodeMetadata(l.meta)
}

// Neighbor is a Location paired with its distance (meters) from the query
// origin, returned by radius/knn/cylinder queries.
type Neighbor struct {
	Location
	Distance float64
}

// TrajectoryPoint is a single historical sample from query_trajectory.
type TrajectoryPoint struct {
	Point     *geom.Point
	Timestamp time.Time
	meta      []byte
}

// Metadata decodes the sample's metadata as a JSON object, or nil if it has
// none. Use MetadataValue for metadata that is not an object.
func (t *TrajectoryPoint) Metadata() (map[string]any, error) {
	return decodeMetadataObject(t.meta)
}

// MetadataValue decodes the sample's metadata as any JSON value, or nil.
func (t *TrajectoryPoint) MetadataValue() (any, error) {
	return decodeMetadata(t.meta)
}

// Stats is a snapshot of database counters.
type Stats struct {
	OperationsCount       uint64 `json:"operations_count"`
	HotStateObjects       uint64 `json:"hot_state_objects"`
	ColdStateTrajectories uint64 `json:"cold_state_trajectories"`
	ColdStateBufferBytes  uint64 `json:"cold_state_buffer_bytes"`
	MemoryUsageBytes      uint64 `json:"memory_usage_bytes"`
}

// Errors

// Sentinel errors for the stable status codes; use errors.Is to match.
var (
	ErrClosed           = errors.New("spatio: database is closed")
	ErrSerialization    = errors.New("spatio: serialization error")
	ErrInvalidTimestamp = errors.New("spatio: invalid timestamp")
	ErrInvalidInput     = errors.New("spatio: invalid input")
	ErrObjectNotFound   = errors.New("spatio: object not found")
	ErrIO               = errors.New("spatio: I/O error")
	ErrNullArgument     = errors.New("spatio: null argument")
	ErrEncoding         = errors.New("spatio: invalid UTF-8")
	ErrOther            = errors.New("spatio: error")
)

// Error wraps a non-OK status code and the native error message. It unwraps to
// the matching sentinel above.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("spatio: %s (code %d)", e.Message, e.Code)
	}
	return fmt.Sprintf("spatio: error (code %d)", e.Code)
}

func (e *Error) Unwrap() error {
	switch e.Code {
	case statusErrClosed:
		return ErrClosed
	case statusErrSerialization:
		return ErrSerialization
	case statusErrInvalidTime:
		return ErrInvalidTimestamp
	case statusErrInvalidInput:
		return ErrInvalidInput
	case statusErrNotFound:
		return ErrObjectNotFound
	case statusErrIO:
		return ErrIO
	case statusErrNullArg:
		return ErrNullArgument
	case statusErrUTF8:
		return ErrEncoding
	default:
		return ErrOther
	}
}

// decode turns a status code plus the error out-pointer into a Go error,
// consuming (and freeing) the native message string.
func decode(code int32, errPtr unsafe.Pointer) error {
	if code == statusOK {
		consumeString(errPtr) // normally null, but free defensively
		return nil
	}
	return &Error{Code: int(code), Message: consumeString(errPtr)}
}

// Options

// Option configures a database at open time.
type Option func(*openConfig)

type openConfig struct {
	BufferCapacity        *int `json:"buffer_capacity,omitempty"`
	PersistenceBufferSize *int `json:"persistence_buffer_size,omitempty"`
}

// WithBufferCapacity sets the updates kept per object. For in-memory databases
// this is all the history they keep.
func WithBufferCapacity(n int) Option {
	return func(c *openConfig) { c.BufferCapacity = &n }
}

// WithPersistenceBufferSize sets how many writes are buffered before flushing.
func WithPersistenceBufferSize(n int) Option {
	return func(c *openConfig) { c.PersistenceBufferSize = &n }
}

// json renders the config to JSON, or nil if no options were set (defaults).
func (c openConfig) json() (*string, error) {
	if c.BufferCapacity == nil && c.PersistenceBufferSize == nil {
		return nil, nil
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// WriteOption configures a single write (upsert).
type WriteOption func(*writeOptions)

type writeOptions struct {
	timestamp *time.Time
}

// WithTimestamp records the location at an explicit time rather than now.
func WithTimestamp(t time.Time) WriteOption {
	return func(w *writeOptions) { w.timestamp = &t }
}

func (w writeOptions) json() (*string, error) {
	if w.timestamp == nil {
		return nil, nil
	}
	payload := map[string]float64{"timestamp": timeToSeconds(*w.timestamp)}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// geom <-> scalar / GeoJSON conversion

func timeToSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

func secondsToTime(secs float64) time.Time {
	sec := int64(secs)
	nsec := int64((secs - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

// pointXYZ extracts longitude, latitude, and altitude from a point. A 2D point
// yields z = 0.
func pointXYZ(p *geom.Point) (x, y, z float64, err error) {
	if p == nil {
		return 0, 0, 0, fmt.Errorf("%w: point is nil", ErrInvalidInput)
	}
	fc := p.FlatCoords()
	if len(fc) < 2 {
		return 0, 0, 0, fmt.Errorf("%w: point has fewer than 2 coordinates", ErrInvalidInput)
	}
	x, y = fc[0], fc[1]
	if zi := p.Layout().ZIndex(); zi != -1 && zi < len(fc) {
		z = fc[zi]
	}
	return x, y, z, nil
}

// newPoint builds an XYZ point in WGS84.
func newPoint(x, y, z float64) *geom.Point {
	return geom.NewPointFlat(geom.XYZ, []float64{x, y, z}).SetSRID(srid4326)
}

// polygonToGeoJSON encodes a polygon for the C ABI.
func polygonToGeoJSON(p *geom.Polygon) (string, error) {
	if p == nil {
		return "", fmt.Errorf("%w: polygon is nil", ErrInvalidInput)
	}
	b, err := geojson.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("spatio: encoding polygon: %w", err)
	}
	return string(b), nil
}

// geoJSONToPolygon decodes GeoJSON returned by the C ABI into a polygon.
func geoJSONToPolygon(s string) (*geom.Polygon, error) {
	var g geom.T
	if err := geojson.Unmarshal([]byte(s), &g); err != nil {
		return nil, fmt.Errorf("spatio: decoding polygon: %w", err)
	}
	poly, ok := g.(*geom.Polygon)
	if !ok {
		return nil, fmt.Errorf("spatio: expected polygon, got %T", g)
	}
	return poly.SetSRID(srid4326), nil
}

// Binary result decoding (mirrors crates/cabi/src/wire.rs)

func decodeMetadata(meta []byte) (any, error) {
	if len(meta) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(meta, &v); err != nil {
		return nil, fmt.Errorf("spatio: decoding metadata: %w", err)
	}
	return v, nil
}

func decodeMetadataObject(meta []byte) (map[string]any, error) {
	v, err := decodeMetadata(meta)
	if v == nil || err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("spatio: metadata is a %T, not an object; use MetadataValue", v)
	}
	return m, nil
}

// reader walks a little-endian result buffer. The buffer is produced by our own
// library, so layout is trusted; out-of-range access would panic, which only a
// version mismatch could trigger.
type reader struct {
	b   []byte
	off int
}

func (r *reader) u32() uint32 {
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *reader) f64() float64 {
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.b[r.off:]))
	r.off += 8
	return v
}

// blob returns a view into the buffer (no copy); callers must copy before the
// buffer is freed if the bytes need to outlive it.
func (r *reader) blob() []byte {
	n := int(r.u32())
	s := r.b[r.off : r.off+n]
	r.off += n
	return s
}

func (r *reader) str() string {
	return string(r.blob()) // copies into a Go string
}

// metaView returns the metadata blob as a sub-slice of the (Go-owned) result
// buffer, without copying. The buffer stays alive as long as any returned record
// holds this slice, so lazy metadata access costs nothing until it's decoded.
func (r *reader) metaView() []byte {
	b := r.blob()
	if len(b) == 0 {
		return nil
	}
	return b
}

func decodeNeighbors(buf []byte, namespace string) []Neighbor {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]Neighbor, n)
	for i := range out {
		x, y, z, ts, dist := r.f64(), r.f64(), r.f64(), r.f64(), r.f64()
		out[i] = Neighbor{
			Location: Location{
				ObjectID:  r.str(),
				Namespace: namespace,
				Point:     newPoint(x, y, z),
				Timestamp: secondsToTime(ts),
				meta:      r.metaView(),
			},
			Distance: dist,
		}
	}
	return out
}

func decodeLocations(buf []byte, namespace string) []Location {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]Location, n)
	for i := range out {
		x, y, z, ts := r.f64(), r.f64(), r.f64(), r.f64()
		out[i] = Location{
			ObjectID:  r.str(),
			Namespace: namespace,
			Point:     newPoint(x, y, z),
			Timestamp: secondsToTime(ts),
			meta:      r.metaView(),
		}
	}
	return out
}

func decodeLocationOne(buf []byte, namespace string) *Location {
	locs := decodeLocations(buf, namespace)
	if len(locs) == 0 {
		return nil
	}
	return &locs[0]
}

func decodeTrajectory(buf []byte) []TrajectoryPoint {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]TrajectoryPoint, n)
	for i := range out {
		x, y, z, ts := r.f64(), r.f64(), r.f64(), r.f64()
		out[i] = TrajectoryPoint{
			Point:     newPoint(x, y, z),
			Timestamp: secondsToTime(ts),
			meta:      r.metaView(),
		}
	}
	return out
}

// Shapes

// ShapeKind identifies the variant of a Shape.
type ShapeKind string

const (
	ShapePolygon  ShapeKind = "polygon"
	ShapeCircle   ShapeKind = "circle"
	ShapeCorridor ShapeKind = "corridor"
)

// Shape is a stored area: a polygon, a circle, or a corridor (everything within
// HalfWidth meters of a line). Build one with PolygonShape, CircleShape, or
// CorridorShape; only the fields for its Kind are set.
type Shape struct {
	Kind ShapeKind

	Polygon *geom.Polygon

	Center *geom.Point
	Radius float64

	Line      *geom.LineString
	HalfWidth float64
}

// PolygonShape wraps a polygon. Edges are straight in lon/lat, as in GeoJSON.
func PolygonShape(p *geom.Polygon) *Shape {
	return &Shape{Kind: ShapePolygon, Polygon: p}
}

// CircleShape is the area within radius meters of center.
func CircleShape(center *geom.Point, radius float64) *Shape {
	return &Shape{Kind: ShapeCircle, Center: center, Radius: radius}
}

// CorridorShape is the area within halfWidth meters of line.
func CorridorShape(line *geom.LineString, halfWidth float64) *Shape {
	return &Shape{Kind: ShapeCorridor, Line: line, HalfWidth: halfWidth}
}

// StoredShape is a shape with its ID, returned by ListShapes.
type StoredShape struct {
	ID    string
	Shape *Shape
}

// ShapeHit is a shape ID and its distance in meters from the query point (0
// inside the shape), returned by ShapesWithin.
type ShapeHit struct {
	ID       string
	Distance float64
}

// shapeJSON mirrors the serde form of spatio::Shape.
type shapeJSON struct {
	Type       ShapeKind      `json:"type"`
	Exterior   [][2]float64   `json:"exterior,omitempty"`
	Interiors  [][][2]float64 `json:"interiors,omitempty"`
	Center     *[2]float64    `json:"center,omitempty"`
	RadiusM    *float64       `json:"radius_m,omitempty"`
	Line       [][2]float64   `json:"line,omitempty"`
	HalfWidthM *float64       `json:"half_width_m,omitempty"`
}

func xyCoords(cs []geom.Coord) [][2]float64 {
	out := make([][2]float64, len(cs))
	for i, c := range cs {
		out[i] = [2]float64{c[0], c[1]}
	}
	return out
}

func flatXY(cs [][2]float64) []float64 {
	out := make([]float64, 0, 2*len(cs))
	for _, c := range cs {
		out = append(out, c[0], c[1])
	}
	return out
}

func (s *Shape) toJSON() (string, error) {
	if s == nil {
		return "", fmt.Errorf("%w: shape is nil", ErrInvalidInput)
	}
	j := shapeJSON{Type: s.Kind}
	switch s.Kind {
	case ShapePolygon:
		if s.Polygon == nil || s.Polygon.NumLinearRings() == 0 {
			return "", fmt.Errorf("%w: polygon shape has no exterior ring", ErrInvalidInput)
		}
		j.Exterior = xyCoords(s.Polygon.LinearRing(0).Coords())
		for i := 1; i < s.Polygon.NumLinearRings(); i++ {
			j.Interiors = append(j.Interiors, xyCoords(s.Polygon.LinearRing(i).Coords()))
		}
	case ShapeCircle:
		x, y, _, err := pointXYZ(s.Center)
		if err != nil {
			return "", err
		}
		j.Center = &[2]float64{x, y}
		j.RadiusM = &s.Radius
	case ShapeCorridor:
		if s.Line == nil {
			return "", fmt.Errorf("%w: corridor line is nil", ErrInvalidInput)
		}
		j.Line = xyCoords(s.Line.Coords())
		j.HalfWidthM = &s.HalfWidth
	default:
		return "", fmt.Errorf("%w: unknown shape kind %q", ErrInvalidInput, s.Kind)
	}
	b, err := json.Marshal(j)
	if err != nil {
		return "", fmt.Errorf("spatio: encoding shape: %w", err)
	}
	return string(b), nil
}

func shapeFromJSON(b []byte) (*Shape, error) {
	var j shapeJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("spatio: decoding shape: %w", err)
	}
	switch j.Type {
	case ShapePolygon:
		ends := []int{2 * len(j.Exterior)}
		flat := flatXY(j.Exterior)
		for _, r := range j.Interiors {
			flat = append(flat, flatXY(r)...)
			ends = append(ends, len(flat))
		}
		return PolygonShape(geom.NewPolygonFlat(geom.XY, flat, ends).SetSRID(srid4326)), nil
	case ShapeCircle:
		if j.Center == nil || j.RadiusM == nil {
			return nil, fmt.Errorf("spatio: decoding shape: incomplete circle")
		}
		center := geom.NewPointFlat(geom.XY, j.Center[:]).SetSRID(srid4326)
		return CircleShape(center, *j.RadiusM), nil
	case ShapeCorridor:
		if j.HalfWidthM == nil {
			return nil, fmt.Errorf("spatio: decoding shape: incomplete corridor")
		}
		line := geom.NewLineStringFlat(geom.XY, flatXY(j.Line)).SetSRID(srid4326)
		return CorridorShape(line, *j.HalfWidthM), nil
	default:
		return nil, fmt.Errorf("spatio: decoding shape: unknown type %q", j.Type)
	}
}

func decodeShapes(buf []byte) ([]StoredShape, error) {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]StoredShape, n)
	for i := range out {
		id := r.str()
		s, err := shapeFromJSON(r.blob())
		if err != nil {
			return nil, err
		}
		out[i] = StoredShape{ID: id, Shape: s}
	}
	return out, nil
}

func decodeShapeIDs(buf []byte) []string {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]string, n)
	for i := range out {
		out[i] = r.str()
	}
	return out
}

func decodeShapeHits(buf []byte) []ShapeHit {
	r := reader{b: buf}
	n := int(r.u32())
	out := make([]ShapeHit, n)
	for i := range out {
		d := r.f64()
		out[i] = ShapeHit{ID: r.str(), Distance: d}
	}
	return out
}
