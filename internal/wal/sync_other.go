//go:build !linux

package wal

import "os"

// syncFile falls back to full fsync where fdatasync is not available via the
// standard library.
func syncFile(f *os.File) error {
	return f.Sync()
}
