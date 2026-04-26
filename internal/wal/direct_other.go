//go:build !linux

package wal

import "os"

// openDirectFile opens a segment file using standard buffered I/O on non-Linux
// systems (such as macOS or Windows), where Linux-specific unix.O_DIRECT is unavailable.
func openDirectFile(path string, flag int, perm os.FileMode, wantDirect bool) (*os.File, bool, error) {
	f, err := os.OpenFile(path, flag, perm)
	return f, false, err
}
