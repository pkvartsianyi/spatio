# Spatio for Go

Go bindings for [Spatio](https://github.com/pkvartsianyi/spatio), a
high-performance embedded spatio-temporal database. The bindings call the Rust
core in-process through its C ABI using
[purego](https://github.com/ebitengine/purego) (no cgo), so there's no C
toolchain needed to build your program. Geometry types come from
[`github.com/twpayne/go-geom`](https://github.com/twpayne/go-geom).

## Install

```bash
go get github.com/pkvartsianyi/spatio/bindings/go
```

The module does not ship the native library (`libs/` is empty in the
published module). Download the asset for your platform from the matching
`cabi-v*` [GitHub release](https://github.com/pkvartsianyi/spatio/releases)
(`libspatio_cabi-<goos>_<goarch>.{so,dylib}`) and point `SPATIO_LIB_PATH` at
it:

```bash
export SPATIO_LIB_PATH=/path/to/libspatio_cabi-linux_amd64.so
```

## Quick start

```go
package main

import (
	"fmt"

	"github.com/twpayne/go-geom"
	spatio "github.com/pkvartsianyi/spatio/bindings/go"
)

func main() {
	db, err := spatio.OpenMemory()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	nyc := geom.NewPointFlat(geom.XY, []float64{-74.0060, 40.7128})
	_ = db.Upsert("cities", "nyc", nyc, map[string]any{"population": 8_000_000})

	nearby, _ := db.QueryRadius("cities", nyc, 100_000, 10)
	for _, n := range nearby {
		fmt.Printf("%s at %.0fm\n", n.ObjectID, n.Distance)
	}
}
```

## Types

| Spatio                | go-geom                          |
| --------------------- | -------------------------------- |
| point                 | `*geom.Point` (XYZ; XY ⇒ z=0)    |
| polygon               | `*geom.Polygon` (via GeoJSON)    |
| trajectory (input)    | `*geom.LineString` layout `XYM`  |
| bounding box (output) | `*geom.Bounds`                   |
| shape                 | `*spatio.Shape` (see below)      |

Geometries returned by the bindings carry SRID 4326 (WGS84). For
`InsertTrajectory`, the line's `M` ordinate holds each sample's timestamp as
unix seconds (use `geom.XYM`); trajectories are stored in 2D, so a non-zero Z is
rejected.

Errors map to sentinel values (`spatio.ErrObjectNotFound`, `spatio.ErrClosed`,
…); match them with `errors.Is`.

## Shapes

Stored areas (geofences, zones, routes) live in their own keyspace per
namespace, so a shape and an object may share an ID. Build one with
`PolygonShape(*geom.Polygon)`, `CircleShape(center *geom.Point, radius)` or
`CorridorShape(line *geom.LineString, halfWidth)` (everything within
`halfWidth` meters of the line). Distances are in meters.

```go
_ = db.SetShape("zones", "depot", spatio.CircleShape(nyc, 500))
_ = db.SetShape("zones", "route", spatio.CorridorShape(
	geom.NewLineStringFlat(geom.XY, []float64{-74.01, 40.70, -74.00, 40.75}), 50))

ids, _ := db.ShapesContaining("zones", nyc, 10)        // []string, sorted
hits, _ := db.ShapesWithin("zones", nyc, 1_000, 10)    // []ShapeHit, nearest first
shape, _ := db.GetShape("zones", "depot")              // *Shape, nil if missing
all, _ := db.ListShapes("zones")                       // []StoredShape, sorted by ID
deleted, _ := db.DeleteShape("zones", "depot")         // false if it did not exist
```

`ShapeHit.Distance` is 0 for shapes containing the point. Invalid shapes
(out-of-range coordinates, a negative radius, a corridor line with fewer than
two points, a polygon ring with fewer than three vertices) return
`ErrInvalidInput`.

## Metadata

`Upsert` accepts any JSON-encodable metadata. `Location` and `TrajectoryPoint`
decode it lazily, so queries that only need positions/distances never pay for
it: `Metadata() (map[string]any, error)` for JSON objects, and
`MetadataValue() (any, error)` for any value (e.g. lists written from Python):

```go
for _, n := range nearby {
	meta, err := n.Metadata()
	if err != nil { /* ... */ }
	_ = meta["population"]
}
```

## Performance

Result sets cross the FFI boundary as a single packed little-endian binary
buffer (allocated once in Rust, read directly in Go via `unsafe.Slice`), not
JSON, so there's no per-record float formatting, JSON envelope, or reflection.
Coordinates are scalars; per-record metadata is an opaque blob decoded only when
`Metadata()` is called. `convex_hull` still returns GeoJSON and shapes cross as
JSON (low-volume, geometry-shaped).

## Native library resolution

At first use the bindings load the platform shared library, in order:

1. `SPATIO_LIB_PATH`, if set, points directly at the library file.
2. Otherwise the library embedded for `GOOS_GOARCH` under `libs/` (present only
   in builds that staged it, e.g. this repo after `just go-build-lib`) is
   extracted to a temp file, loaded, and the temp file removed.

The library's result-buffer layout version must match the bindings, otherwise
loading fails with an error.

## Developing in this repo

The shared library is built from the `spatio-cabi` crate and staged where
`go:embed` expects it:

```bash
just go-build-lib      # cargo build -p spatio-cabi --release + stage into libs/
just go-test           # builds the lib, then `go test ./...`
just go-example        # runs examples/basic
```

Or point at a hand-built library:

```bash
cargo build -p spatio-cabi --release
SPATIO_LIB_PATH=target/release/libspatio_cabi.dylib go test ./bindings/go/...
```

Platform binaries under `libs/<goos>_<goarch>/` are git-ignored; CI and the
release pipeline build them for each target.
