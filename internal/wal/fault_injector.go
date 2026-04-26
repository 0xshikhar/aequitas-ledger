package wal

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var (
	ErrInjectedENOSPC     = syscall.ENOSPC
	ErrInjectedShortWrite = errors.New("wal: fault-injected short write")
)

// FaultInjector provides tools for simulating hardware failure, bit flips,
// torn writes, and disk exhaustion on WAL segments (T3.2).
type FaultInjector struct{}

func NewFaultInjector() *FaultInjector {
	return &FaultInjector{}
}

// FlipBit reads the byte at the specified file offset, inverts the chosen bit
// (0-7), and writes it back to disk.
func (fi *FaultInjector) FlipBit(path string, byteOffset int64, bitIndex uint) error {
	if bitIndex > 7 {
		return fmt.Errorf("bitIndex must be between 0 and 7, got %d", bitIndex)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open segment for bit flip: %w", err)
	}
	defer f.Close()

	var b [1]byte
	if _, err := f.ReadAt(b[:], byteOffset); err != nil {
		return fmt.Errorf("read byte at offset %d: %w", byteOffset, err)
	}
	b[0] ^= 1 << bitIndex
	if _, err := f.WriteAt(b[:], byteOffset); err != nil {
		return fmt.Errorf("write flipped byte at offset %d: %w", byteOffset, err)
	}
	return f.Sync()
}

// CorruptBytes writes corrupting garbage bytes at the specified offset.
func (fi *FaultInjector) CorruptBytes(path string, offset int64, corruptData []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open segment for byte corruption: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteAt(corruptData, offset); err != nil {
		return fmt.Errorf("write corrupted bytes at %d: %w", offset, err)
	}
	return f.Sync()
}

// TornWrite truncates or overwrites the end of a segment to simulate a torn write
// occurring mid-frame during sudden power loss.
func (fi *FaultInjector) TornWrite(path string, cutOffset int64) error {
	return os.Truncate(path, cutOffset)
}
