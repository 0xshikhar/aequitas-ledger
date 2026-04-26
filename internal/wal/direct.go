package wal

import (
	"fmt"
	"unsafe"
)

// DirectIOBlockSize is the standard 4KB block alignment required by Linux O_DIRECT.
const DirectIOBlockSize = 4096

// NewAlignedBuffer allocates a byte slice of the requested size whose start address
// is aligned to a multiple of alignment (typically DirectIOBlockSize).
func NewAlignedBuffer(size int, alignment int) []byte {
	if alignment <= 0 {
		return make([]byte, size)
	}
	buf := make([]byte, size+alignment)
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	offset := (alignment - int(ptr%uintptr(alignment))) % alignment
	return buf[offset : offset+size]
}

// IsAligned checks whether the byte slice's backing pointer is aligned to alignment.
func IsAligned(p []byte, alignment int) bool {
	if len(p) == 0 || alignment <= 0 {
		return true
	}
	ptr := uintptr(unsafe.Pointer(&p[0]))
	return ptr%uintptr(alignment) == 0
}

// directIOBuffer provides 4KB-aligned buffering for O_DIRECT writes.
type directIOBuffer struct {
	block       []byte
	blockOffset int64 // file offset corresponding to the start of block
	buffered    int   // bytes currently valid in block
}

func newDirectIOBuffer(initialFileOffset int64) *directIOBuffer {
	// Align to lower 4KB boundary
	alignedOffset := (initialFileOffset / DirectIOBlockSize) * DirectIOBlockSize
	return &directIOBuffer{
		block:       NewAlignedBuffer(DirectIOBlockSize, DirectIOBlockSize),
		blockOffset: alignedOffset,
		buffered:    int(initialFileOffset - alignedOffset),
	}
}

// flushTo ensures all bytes up to targetOffset are written to f using 4KB blocks.
func (d *directIOBuffer) flush(f interface{ WriteAt([]byte, int64) (int, error) }, targetOffset int64) error {
	if d.blockOffset+int64(d.buffered) <= d.blockOffset && targetOffset == d.blockOffset {
		return nil
	}
	// Zero out unwritten padding in the remainder of the 4KB block for determinism.
	if d.buffered < DirectIOBlockSize {
		for i := d.buffered; i < DirectIOBlockSize; i++ {
			d.block[i] = 0
		}
	}
	// Write the 4KB block
	_, err := f.WriteAt(d.block, d.blockOffset)
	if err != nil {
		return fmt.Errorf("direct io write at %d: %w", d.blockOffset, err)
	}
	return nil
}
