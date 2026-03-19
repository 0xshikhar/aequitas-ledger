package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const DefaultSegmentSize int64 = 64 << 20 // 64 MiB

var ErrSegmentFull = errors.New("wal: segment full")

type Segment struct {
	id          int
	path        string
	file        *os.File
	maxSize     int64
	writeOffset int64
}

func openSegment(dir string, id int, maxSize int64) (*Segment, error) {
	if maxSize <= 0 {
		maxSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	path := segmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}

	return &Segment{
		id:          id,
		path:        path,
		file:        f,
		maxSize:     maxSize,
		writeOffset: info.Size(),
	}, nil
}

func segmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%06d.seg", id))
}

func (s *Segment) Write(p []byte) (int64, error) {
	if s.IsFull(len(p)) {
		return s.writeOffset, ErrSegmentFull
	}
	start := s.writeOffset
	n, err := s.file.WriteAt(p, start)
	s.writeOffset += int64(n)
	if err != nil {
		return start, err
	}
	if n != len(p) {
		return start, ioErrShortWrite(n, len(p))
	}
	return start, nil
}

func (s *Segment) ReadAt(p []byte, offset int64) (int, error) {
	return s.file.ReadAt(p, offset)
}

func (s *Segment) IsFull(nextWriteBytes int) bool {
	if nextWriteBytes < 0 {
		return true
	}
	return s.writeOffset+int64(nextWriteBytes) > s.maxSize
}

func (s *Segment) Sync() error {
	return s.file.Sync()
}

func (s *Segment) Truncate(size int64) error {
	if size < 0 {
		size = 0
	}
	if err := s.file.Truncate(size); err != nil {
		return err
	}
	s.writeOffset = size
	return nil
}

func (s *Segment) Close() error {
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *Segment) Path() string {
	return s.path
}

func (s *Segment) Size() int64 {
	return s.writeOffset
}

func ioErrShortWrite(wrote, expected int) error {
	return fmt.Errorf("wal: short write wrote=%d expected=%d", wrote, expected)
}
