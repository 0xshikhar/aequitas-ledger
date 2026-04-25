package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
// It assigns a monotonic LSN to each record and appends a BatchCommit record at the end.
// It does not call fsync; call Sync exactly once after this for group commit.
func (w *WAL) AppendBatch(records []Record) (int64, error) {
	if len(records) == 0 {
		return w.currentLSN, nil
	}

	startLSN := w.currentLSN + 1
	for i := range records {
		w.currentLSN++
		records[i].LSN = uint64(w.currentLSN)
		encoded, err := EncodeRecord(records[i])
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

	// Append BatchCommit marker record to frame the batch atomically
	w.currentLSN++
	var commitPayload [12]byte
	binary.BigEndian.PutUint64(commitPayload[0:8], uint64(w.currentLSN))
	binary.BigEndian.PutUint32(commitPayload[8:12], uint32(len(records)))
	commitRec := Record{
		LSN:     uint64(w.currentLSN),
		Type:    RecordTypeBatchCommit,
		Payload: commitPayload[:],
	}
	encodedCommit, err := EncodeRecord(commitRec)
	if err != nil {
		return 0, err
	}
	if w.current.IsFull(len(encodedCommit)) {
		if err := w.Rotate(); err != nil {
			return 0, err
		}
	}
	if _, err := w.current.Write(encodedCommit); err != nil {
		return 0, err
	}

	return startLSN, nil
}

func (w *WAL) Sync() error {
	if w.current == nil {
		return nil
	}
	return w.current.Sync()
}

func (w *WAL) Dir() string {
	return w.dir
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

func (w *WAL) AppendCheckpoint(lsn int64) error {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(lsn))
	rec := Record{
		Type:    RecordTypeCheckpoint,
		Payload: payload[:],
	}
	_, err := w.AppendBatch([]Record{rec})
	if err != nil {
		return err
	}
	return w.Sync()
}

func (w *WAL) TruncateBefore(lsn int64) error {
	ids, err := listSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if id >= w.currentID {
			continue // Do not delete active current segment
		}
		path := filepath.Join(w.dir, fmt.Sprintf("wal-%06d.seg", id))
		maxLSN, err := peekSegmentMaxLSN(path)
		if err != nil || maxLSN < lsn {
			_ = os.Remove(path)
		}
	}
	return nil
}

func peekSegmentMaxLSN(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() < int64(RecordHeaderSize+RecordCRCSize) {
		return 0, err
	}

	// Read last record header if possible, or scan
	buf := make([]byte, stat.Size())
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}

	var maxLSN int64
	offset := int64(0)
	size := stat.Size()
	for offset < size {
		if size-offset < int64(RecordHeaderSize+RecordCRCSize) {
			break
		}
		lsn := int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
		payloadLen := int64(binary.BigEndian.Uint32(buf[offset+9 : offset+13]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			break
		}
		maxLSN = lsn
		offset += recLen
	}
	return maxLSN, nil
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
	return w.RecoverFromLSN(0, handler)
}

func (w *WAL) RecoverFromLSN(fromLSN int64, handler func(Record) error) error {
	filterHandler := func(r Record) error {
		if int64(r.LSN) <= fromLSN {
			return nil
		}
		return handler(r)
	}
	recovered, lastSegmentID, err := recoverSegments(w.dir, filterHandler)
	if err != nil {
		return err
	}
	if recovered > w.currentLSN {
		w.currentLSN = recovered
	}

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
