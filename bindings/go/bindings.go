// Package spatio provides Go bindings for the Spatio embedded spatio-temporal
// database. It loads the prebuilt Rust C-ABI shared library via purego (no
// cgo) and exposes an idiomatic API built on github.com/twpayne/go-geom.
package spatio

import (
	"embed"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Prebuilt libraries, one per platform, populated by CI / `just go-build-lib`
// into libs/<goos>_<goarch>/. Embedding the directory keeps `go get` working
// without a C toolchain.
//
//go:embed libs
var embeddedLibs embed.FS

// Status codes returned by the C ABI (must match crates/cabi/src/ffi.rs).
const (
	statusOK               = 0
	statusErrClosed        = 1
	statusErrSerialization = 2
	statusErrInvalidTime   = 3
	statusErrInvalidInput  = 4
	statusErrNotFound      = 5
	statusErrIO            = 6
	statusErrOther         = 7
	statusErrNullArg       = 8
	statusErrUTF8          = 9
)

// C ABI entry points, bound at load time by registerLibrary.
var (
	fnVersion    func() unsafe.Pointer
	fnStringFree func(unsafe.Pointer)
	fnBufferFree func(ptr unsafe.Pointer, length uintptr)

	fnOpenMemory func(cfg, outHandle, errOut unsafe.Pointer) int32
	fnOpen       func(path, cfg, outHandle, errOut unsafe.Pointer) int32
	fnClose      func(h uintptr, errOut unsafe.Pointer) int32

	fnUpsert           func(h uintptr, ns, id unsafe.Pointer, x, y, z float64, meta, opts, errOut unsafe.Pointer) int32
	fnDelete           func(h uintptr, ns, id, errOut unsafe.Pointer) int32
	fnInsertTrajectory func(h uintptr, ns, id, traj, errOut unsafe.Pointer) int32

	// Result-set functions write a packed binary buffer (ptr,len) the caller
	// frees with fnBufferFree. See wire.rs for the layout.
	fnGet   func(h uintptr, ns, id, outPtr, outLen, errOut unsafe.Pointer) int32
	fnStats func(h uintptr, outArr, errOut unsafe.Pointer) int32

	fnQueryRadius       func(h uintptr, ns unsafe.Pointer, x, y, z, radius float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryNear         func(h uintptr, ns, id unsafe.Pointer, radius float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnKNN               func(h uintptr, ns unsafe.Pointer, x, y, z float64, k int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnKNNNearObject     func(h uintptr, ns, id unsafe.Pointer, k int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryBBox         func(h uintptr, ns unsafe.Pointer, minX, minY, maxX, maxY float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryCylinder     func(h uintptr, ns unsafe.Pointer, x, y, minZ, maxZ, radius float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryBBox3D       func(h uintptr, ns unsafe.Pointer, minX, minY, minZ, maxX, maxY, maxZ float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryBBoxNear     func(h uintptr, ns, id unsafe.Pointer, width, height float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryCylinderNear func(h uintptr, ns, id unsafe.Pointer, minZ, maxZ, radius float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryBBox3DNear   func(h uintptr, ns, id unsafe.Pointer, width, height, depth float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryPolygon      func(h uintptr, ns, geojson unsafe.Pointer, limit int, outPtr, outLen, errOut unsafe.Pointer) int32
	fnQueryTrajectory   func(h uintptr, ns, id unsafe.Pointer, start, end float64, limit int, outPtr, outLen, errOut unsafe.Pointer) int32

	fnDistanceBetween func(h uintptr, ns, id1, id2, metric, outDist, outFound, errOut unsafe.Pointer) int32
	fnDistanceTo      func(h uintptr, ns, id unsafe.Pointer, x, y float64, metric, outDist, outFound, errOut unsafe.Pointer) int32
	fnConvexHull      func(h uintptr, ns, outGeoJSON, errOut unsafe.Pointer) int32
	fnBoundingBox     func(h uintptr, ns, outMinX, outMinY, outMaxX, outMaxY, outFound, errOut unsafe.Pointer) int32
)

var (
	loadOnce sync.Once
	loadErr  error
)

// ensureLoaded loads and binds the native library exactly once.
func ensureLoaded() error {
	loadOnce.Do(func() {
		lib, err := loadLibrary()
		if err != nil {
			loadErr = err
			return
		}
		registerLibrary(lib)
	})
	return loadErr
}

// platformLibName returns the shared-library filename for the current OS.
// Only Linux and macOS are supported.
func platformLibName() string {
	if runtime.GOOS == "darwin" {
		return "libspatio_cabi.dylib"
	}
	return "libspatio_cabi.so"
}

// loadLibrary resolves the shared library: an explicit SPATIO_LIB_PATH wins
// (handy for development against a freshly built lib); otherwise the embedded
// per-platform copy is extracted to a temp file and loaded.
func loadLibrary() (uintptr, error) {
	if override := os.Getenv("SPATIO_LIB_PATH"); override != "" {
		return dlopen(override)
	}

	name := platformLibName()
	platform := runtime.GOOS + "_" + runtime.GOARCH
	data, err := embeddedLibs.ReadFile(path.Join("libs", platform, name))
	if err != nil {
		return 0, fmt.Errorf("spatio: no embedded library for %s/%s and SPATIO_LIB_PATH is unset; "+
			"build it with `just go-build-lib` or set SPATIO_LIB_PATH: %w", runtime.GOOS, runtime.GOARCH, err)
	}

	tmpDir, err := os.MkdirTemp("", "spatio-lib-")
	if err != nil {
		return 0, fmt.Errorf("spatio: creating temp dir for native library: %w", err)
	}
	libPath := filepath.Join(tmpDir, name)
	if err := os.WriteFile(libPath, data, 0o600); err != nil {
		return 0, fmt.Errorf("spatio: writing native library: %w", err)
	}
	return dlopen(libPath)
}

// registerLibrary binds every C ABI symbol. Symbol names must match the
// #[unsafe(no_mangle)] functions in crates/cabi/src/lib.rs.
func registerLibrary(lib uintptr) {
	purego.RegisterLibFunc(&fnVersion, lib, "spatio_version")
	purego.RegisterLibFunc(&fnStringFree, lib, "spatio_string_free")
	purego.RegisterLibFunc(&fnBufferFree, lib, "spatio_buffer_free")

	purego.RegisterLibFunc(&fnOpenMemory, lib, "spatio_open_memory")
	purego.RegisterLibFunc(&fnOpen, lib, "spatio_open")
	purego.RegisterLibFunc(&fnClose, lib, "spatio_close")

	purego.RegisterLibFunc(&fnUpsert, lib, "spatio_upsert")
	purego.RegisterLibFunc(&fnDelete, lib, "spatio_delete")
	purego.RegisterLibFunc(&fnInsertTrajectory, lib, "spatio_insert_trajectory")

	purego.RegisterLibFunc(&fnGet, lib, "spatio_get")
	purego.RegisterLibFunc(&fnStats, lib, "spatio_stats")

	purego.RegisterLibFunc(&fnQueryRadius, lib, "spatio_query_radius")
	purego.RegisterLibFunc(&fnQueryNear, lib, "spatio_query_near")
	purego.RegisterLibFunc(&fnKNN, lib, "spatio_knn")
	purego.RegisterLibFunc(&fnKNNNearObject, lib, "spatio_knn_near_object")
	purego.RegisterLibFunc(&fnQueryBBox, lib, "spatio_query_bbox")
	purego.RegisterLibFunc(&fnQueryCylinder, lib, "spatio_query_within_cylinder")
	purego.RegisterLibFunc(&fnQueryBBox3D, lib, "spatio_query_within_bbox_3d")
	purego.RegisterLibFunc(&fnQueryBBoxNear, lib, "spatio_query_bbox_near_object")
	purego.RegisterLibFunc(&fnQueryCylinderNear, lib, "spatio_query_cylinder_near_object")
	purego.RegisterLibFunc(&fnQueryBBox3DNear, lib, "spatio_query_bbox_3d_near_object")
	purego.RegisterLibFunc(&fnQueryPolygon, lib, "spatio_query_polygon")
	purego.RegisterLibFunc(&fnQueryTrajectory, lib, "spatio_query_trajectory")

	purego.RegisterLibFunc(&fnDistanceBetween, lib, "spatio_distance_between")
	purego.RegisterLibFunc(&fnDistanceTo, lib, "spatio_distance_to")
	purego.RegisterLibFunc(&fnConvexHull, lib, "spatio_convex_hull")
	purego.RegisterLibFunc(&fnBoundingBox, lib, "spatio_bounding_box")
}

// Version returns the version of the loaded native library.
func Version() (string, error) {
	if err := ensureLoaded(); err != nil {
		return "", err
	}
	v := goStringAt(fnVersion())
	return v, nil
}

// ---------------------------------------------------------------------------
// C string marshaling
// ---------------------------------------------------------------------------

// cString holds a null-terminated copy of a Go string for passing to C. The
// backing slice must be kept alive (via runtime.KeepAlive) until the call that
// uses ptr() returns.
type cString struct {
	buf []byte
}

func newCString(s string) cString {
	b := make([]byte, len(s)+1)
	copy(b, s)
	return cString{buf: b}
}

// optCString builds a C string from an optional Go string; a nil input yields a
// null pointer.
func optCString(s *string) cString {
	if s == nil {
		return cString{}
	}
	return newCString(*s)
}

func (c cString) ptr() unsafe.Pointer {
	if c.buf == nil {
		return nil
	}
	return unsafe.Pointer(&c.buf[0])
}

// goStringAt copies a NUL-terminated C string at p into a Go string without
// freeing it. p points to library-owned memory, not Go memory.
func goStringAt(p unsafe.Pointer) string {
	if p == nil {
		return ""
	}
	var n int
	for *(*byte)(unsafe.Add(p, n)) != 0 {
		n++
	}
	return string(unsafe.Slice((*byte)(p), n))
}

// consumeString copies the C string at p and frees the underlying allocation.
func consumeString(p unsafe.Pointer) string {
	if p == nil {
		return ""
	}
	s := goStringAt(p)
	fnStringFree(p)
	return s
}
