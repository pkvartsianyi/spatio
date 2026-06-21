package spatio

import (
	"encoding/json"
	"errors"
	"fmt"
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

// Location is an object's current position and metadata.
type Location struct {
	ObjectID  string
	Namespace string
	Point     *geom.Point
	Metadata  map[string]any
	Timestamp time.Time
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
	Metadata  map[string]any
}

// Stats is a snapshot of database counters.
type Stats struct {
	ExpiredCount          uint64 `json:"expired_count"`
	OperationsCount       uint64 `json:"operations_count"`
	SizeBytes             uint64 `json:"size_bytes"`
	HotStateObjects       uint64 `json:"hot_state_objects"`
	ColdStateTrajectories uint64 `json:"cold_state_trajectories"`
	ColdStateBufferBytes  uint64 `json:"cold_state_buffer_bytes"`
	MemoryUsageBytes      uint64 `json:"memory_usage_bytes"`
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Option configures a database at open time.
type Option func(*openConfig)

type openConfig struct {
	BufferCapacity        *int `json:"buffer_capacity,omitempty"`
	PersistenceBufferSize *int `json:"persistence_buffer_size,omitempty"`
}

// WithBufferCapacity sets the per-object recent-history buffer size.
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

// ---------------------------------------------------------------------------
// geom <-> scalar / GeoJSON conversion
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// JSON result shapes (mirror crates/cabi/src/dto.rs)
// ---------------------------------------------------------------------------

type rawLocation struct {
	ObjectID  string         `json:"object_id"`
	Namespace string         `json:"namespace"`
	X         float64        `json:"x"`
	Y         float64        `json:"y"`
	Z         float64        `json:"z"`
	Metadata  map[string]any `json:"metadata"`
	Timestamp float64        `json:"timestamp"`
	Distance  float64        `json:"distance"` // present only for neighbor results
}

func (r rawLocation) location() Location {
	return Location{
		ObjectID:  r.ObjectID,
		Namespace: r.Namespace,
		Point:     newPoint(r.X, r.Y, r.Z),
		Metadata:  r.Metadata,
		Timestamp: secondsToTime(r.Timestamp),
	}
}

type rawTrajectoryPoint struct {
	X         float64        `json:"x"`
	Y         float64        `json:"y"`
	Timestamp float64        `json:"timestamp"`
	Metadata  map[string]any `json:"metadata"`
}

func parseLocations(jsonStr string) ([]Location, error) {
	var raws []rawLocation
	if err := json.Unmarshal([]byte(jsonStr), &raws); err != nil {
		return nil, fmt.Errorf("spatio: decoding result: %w", err)
	}
	out := make([]Location, len(raws))
	for i, r := range raws {
		out[i] = r.location()
	}
	return out, nil
}

func parseNeighbors(jsonStr string) ([]Neighbor, error) {
	var raws []rawLocation
	if err := json.Unmarshal([]byte(jsonStr), &raws); err != nil {
		return nil, fmt.Errorf("spatio: decoding result: %w", err)
	}
	out := make([]Neighbor, len(raws))
	for i, r := range raws {
		out[i] = Neighbor{Location: r.location(), Distance: r.Distance}
	}
	return out, nil
}

func parseTrajectory(jsonStr string) ([]TrajectoryPoint, error) {
	var raws []rawTrajectoryPoint
	if err := json.Unmarshal([]byte(jsonStr), &raws); err != nil {
		return nil, fmt.Errorf("spatio: decoding trajectory: %w", err)
	}
	out := make([]TrajectoryPoint, len(raws))
	for i, r := range raws {
		out[i] = TrajectoryPoint{
			Point:     geom.NewPointFlat(geom.XY, []float64{r.X, r.Y}).SetSRID(srid4326),
			Timestamp: secondsToTime(r.Timestamp),
			Metadata:  r.Metadata,
		}
	}
	return out, nil
}
