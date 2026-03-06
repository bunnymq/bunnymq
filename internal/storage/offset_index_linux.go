//go:build linux

package storage

import (
	"log"
	"os"

	"golang.org/x/sys/unix"
)

func preallocateIndex(f *os.File, size int64) error {
	if err := f.Truncate(size); err != nil {
		return err
	}
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size); err != nil {
		log.Printf("[WARN] storage: fallocate failed (%v); index file blocks may not be pre-allocated", err)
	}
	return nil
}
