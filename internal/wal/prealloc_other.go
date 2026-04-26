//go:build !darwin && !linux

package wal

import "os"

// preallocate is a no-op fallback on operating systems without standard
// fallocate or F_PREALLOCATE syscalls (S2.2).
func preallocate(f *os.File, size int64) error {
	return nil
}
