//go:build !linux

package storage

import (
	"log"
	"os"
)

func preallocateIndex(f *os.File, size int64) error {
	log.Printf("[WARN] storage: fallocate unavailable on this platform; using truncate for index pre-allocation")
	return f.Truncate(size)
}
