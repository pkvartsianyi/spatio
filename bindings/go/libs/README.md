# Native libraries

Prebuilt Spatio C-ABI libraries live here, one directory per platform:

    libs/<goos>_<goarch>/libspatio_cabi.{so,dylib}    (Linux and macOS only)

These are produced by `just go-build-lib` (local) or the release CI matrix.
This README is also what keeps `//go:embed libs` valid before any binary is
built. Platform binaries are git-ignored; for local development either run
`just go-build-lib` or set `SPATIO_LIB_PATH` to a built library.
