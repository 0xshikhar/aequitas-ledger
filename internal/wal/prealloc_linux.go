//go:build linux

package wal

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// preallocate allocates extents using Linux fallocate with FALLOC_FL_KEEP_SIZE (S2.2).
// This pre-commits storage blocks on the filesystem (preventing runtime fragmentation
// and ENOSPC during high-frequency writes) without altering the logical file size
// seen by stat() or recovery.
func preallocate(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
	if err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
			// Filesystem (e.g. tmpfs or non-extent FS) does not support fallocate; proceed gracefully.
			return nil
		}
		return err
	}
	return nil
}
