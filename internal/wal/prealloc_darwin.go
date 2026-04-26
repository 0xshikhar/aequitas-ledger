//go:build darwin

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves contiguous disk blocks for the segment using macOS
// fcntl(F_PREALLOCATE), eliminating runtime block allocation locks and ENOSPC
// during writes. It does not alter the logical file size (S2.2).
func preallocate(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	fstore := unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &fstore)
	if err != nil {
		// Contiguous allocation failed (e.g., fragmented disk); retry with non-contiguous.
		fstore.Flags = unix.F_ALLOCATEALL
		if err2 := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &fstore); err2 != nil {
			return err2
		}
	}
	return nil
}
