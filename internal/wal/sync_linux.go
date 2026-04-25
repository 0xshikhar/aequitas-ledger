//go:build linux

package wal

import (
	"os"
	"syscall"
)

// syncFile uses fdatasync on Linux: appends need the data and the file size
// durable, but not metadata like mtime — a cheaper durability barrier than
// full fsync (S2.1).
func syncFile(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
