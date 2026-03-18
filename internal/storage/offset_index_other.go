//go:build !linux

package storage

import "os"

// preallocateIndex always falls back to truncate on non-Linux platforms.
// Returns (true, nil) to indicate the caller should log a fallocate-fallback warn.
func preallocateIndex(f *os.File, size int64) (bool, error) {
	return true, f.Truncate(size)
}
