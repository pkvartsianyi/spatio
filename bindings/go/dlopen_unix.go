//go:build darwin || linux

package spatio

import (
	"fmt"

	"github.com/ebitengine/purego"
)

// dlopen loads a shared library by path on Unix-like systems.
func dlopen(path string) (uintptr, error) {
	lib, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return 0, fmt.Errorf("spatio: dlopen %q: %w", path, err)
	}
	return lib, nil
}
