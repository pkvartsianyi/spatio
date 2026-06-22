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

Geometries returned by the bindings carry SRID 4326 (WGS84). For
`InsertTrajectory`, the line's `M` ordinate holds each sample's timestamp as
unix seconds (use `geom.XYM`).

Errors map to sentinel values (`spatio.ErrObjectNotFound`, `spatio.ErrClosed`,
…); match them with `errors.Is`.

## Metadata

`Location` and `TrajectoryPoint` expose metadata lazily via a `Metadata()
(map[string]any, error)` method rather than an eager field, so queries that only
need positions/distances never pay to decode it:

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
`Metadata()` is called. `convex_hull` still returns GeoJSON (low-volume,
geometry-shaped).

## Native library resolution

At first use the bindings load the platform shared library, in order:

1. `SPATIO_LIB_PATH`, if set, points directly at the library file.
2. Otherwise the library embedded for `GOOS_GOARCH` under `libs/` is extracted
   to a temp file and loaded.

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
