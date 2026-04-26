//go:build linux

package wal

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// openDirectFile attempts to open the segment with unix.O_DIRECT. If the underlying
// filesystem does not support direct I/O (e.g., tmpfs, overlayfs, NFS), it falls back
// gracefully to standard buffered I/O (S2.2).
func openDirectFile(path string, flag int, perm os.FileMode, wantDirect bool) (*os.File, bool, error) {
	if !wantDirect {
		f, err := os.OpenFile(path, flag, perm)
		return f, false, err
	}

	f, err := os.OpenFile(path, flag|unix.O_DIRECT, perm)
	if err == nil {
		return f, true, nil
	}

	// If O_DIRECT failed due to lack of filesystem support, fall back to buffered I/O.
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		f, fallbackErr := os.OpenFile(path, flag, perm)
		return f, false, fallbackErr
	}

	return nil, false, err
}
