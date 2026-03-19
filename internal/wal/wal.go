package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type WAL struct {
	dir         string
	segmentSize int64
	current     *Segment
	currentID   int
	currentLSN  int64
}

func Open(dir string, segmentSize int64) (*WAL, error) {
	if segmentSize <= 0 {
		segmentSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	ids, err := listSegmentIDs(dir)
	if err != nil {
		return nil, err
	}
	currentID := 1
	if len(ids) > 0 {
		currentID = ids[len(ids)-1]
	}

	seg, err := openSegment(dir, currentID, segmentSize)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:         dir,
		segmentSize: segmentSize,
		current:     seg,
		currentID:   currentID,
	}, nil
}

// AppendBatch serializes all records and appends them to segments.
// It does not call fsync; call Sync exactly once after this for group commit.
func (w *WAL) AppendBatch(records []Record) (int64, error) {
	if len(records) == 0 {
		return w.currentLSN, nil
	}

	startLSN := w.currentLSN + 1
	for _, rec := range records {
		encoded, err := EncodeRecord(rec)
		if err != nil {
			return 0, err
		}
		if len(encoded) > int(w.segmentSize) {
			return 0, fmt.Errorf("wal: record larger than segment size: %d > %d", len(encoded), w.segmentSize)
		}

		if w.current.IsFull(len(encoded)) {
			if err := w.Rotate(); err != nil {
				return 0, err
			}
		}
		if _, err := w.current.Write(encoded); err != nil {
			if errors.Is(err, ErrSegmentFull) {
				if rerr := w.Rotate(); rerr != nil {
					return 0, rerr
				}
				if _, werr := w.current.Write(encoded); werr != nil {
					return 0, werr
				}
				continue
			}
			return 0, err
		}
	}

	w.currentLSN++
	return startLSN, nil
}

func (w *WAL) Sync() error {
	if w.current == nil {
		return nil
	}
	return w.current.Sync()
}

func (w *WAL) CurrentLSN() int64 {
	return w.currentLSN
}

func (w *WAL) Close() error {
	if w.current == nil {
		return nil
	}
	return w.current.Close()
}

func (w *WAL) Rotate() error {
	if w.current != nil {
		if err := w.current.Sync(); err != nil {
			return fmt.Errorf("sync current segment: %w", err)
		}
		if err := w.current.Close(); err != nil {
			return fmt.Errorf("close current segment: %w", err)
		}
	}

	w.currentID++
	seg, err := openSegment(w.dir, w.currentID, w.segmentSize)
	if err != nil {
		return err
	}
	w.current = seg
	return nil
}

func (w *WAL) Recover(handler func(Record) error) error {
	recovered, lastSegmentID, err := recoverSegments(w.dir, handler)
	if err != nil {
		return err
	}
	w.currentLSN = recovered

	if w.current != nil {
		_ = w.current.Close()
	}
	if lastSegmentID == 0 {
		lastSegmentID = 1
	}
	seg, err := openSegment(w.dir, lastSegmentID, w.segmentSize)
	if err != nil {
		return err
	}
	w.currentID = lastSegmentID
	w.current = seg
	return nil
}

func listSegmentIDs(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	ids := make([]int, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".seg")
		id, err := strconv.Atoi(idStr)
		if err != nil || id <= 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, nil
}

func encodeBatch(records []Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	for _, r := range records {
		enc, err := EncodeRecord(r)
		if err != nil {
			return nil, err
		}
		_, _ = buf.Write(enc)
	}
	return buf.Bytes(), nil
}
