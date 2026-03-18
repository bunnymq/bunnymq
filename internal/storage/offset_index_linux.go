//go:build linux

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocateIndex pre-allocates the index file to size bytes using fallocate.
// Returns (true, nil) when fallocate is unsupported and the caller fell back to
// truncate only (caller should log a warn); returns (false, nil) on success.
func preallocateIndex(f *os.File, size int64) (bool, error) {
	if err := f.Truncate(size); err != nil {
		return false, err
	}
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size); err != nil {
		return true, nil
	}
	return false, nil
}
