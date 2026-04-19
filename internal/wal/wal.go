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

// AdvanceLSNTo raises the in-memory LSN watermark when recovery state (e.g. a
// loaded snapshot) proves the WAL head is behind it, so new records never reuse
// LSNs at or below an already-persisted snapshot.
func (w *WAL) AdvanceLSNTo(atLeast int64) {
	if atLeast > w.currentLSN {
		w.currentLSN = atLeast
	}
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
		if err != nil {
			continue // cannot prove the segment is below lsn; never delete blindly
		}
		if maxLSN < lsn {
			_ = os.Remove(path)
		}
	}
	return nil
}

const (
	// peekTailChunk bounds how much of a segment's tail peekSegmentMaxLSN reads.
	// The last record's frame starts within this window for any realistic record.
	peekTailChunk = 64 << 10
	// peekResyncBound bounds how far into the tail chunk the frame-chain resync
	// scans before settling for the furthest chain found.
	peekResyncBound = 4 << 10
	// peekMaxPayload bounds payload length while frame-walking the tail.
	peekMaxPayload = 1 << 20
)

// peekSegmentMaxLSN returns the LSN of the last structurally valid record in a
// segment by reading only its tail. TruncateBefore uses it to decide whether a
// segment lies entirely below a snapshot LSN; recovery remains the authority on
// integrity, so frames are walked structurally (no per-record CRC), matching
// the previous full-scan semantics at O(tail) cost instead of O(segment).
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
	size := stat.Size()

	chunk := int64(peekTailChunk)
	if chunk > size {
		chunk = size
	}
	buf := make([]byte, chunk)
	if _, err := f.ReadAt(buf, size-chunk); err != nil && err != io.EOF {
		return 0, err
	}

	// Frames are back-to-back and the last one ends at EOF, but the chunk
	// usually starts mid-frame: resync by finding the earliest offset from
	// which a chain of structurally valid frames walks furthest toward EOF.
	bestEnd, bestLSN := 0, int64(0)
	limit := min(len(buf), peekResyncBound)
	for start := 0; start < limit; start++ {
		end, lsn := walkFrameChain(buf[start:])
		if end > bestEnd {
			bestEnd, bestLSN = end, lsn
		}
		if end == len(buf)-start {
			break // chain reaches EOF; this is the last record's LSN
		}
	}
	return bestLSN, nil
}

// walkFrameChain walks structurally valid [LSN|type|len|payload|crc] frames and
// returns the bytes covered and the LSN of the last complete frame.
func walkFrameChain(b []byte) (int, int64) {
	off, lastLSN := 0, int64(0)
	for off+RecordHeaderSize+RecordCRCSize <= len(b) {
		payloadLen := int64(binary.BigEndian.Uint32(b[off+9 : off+13]))
		if payloadLen > peekMaxPayload {
			break
		}
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if off+int(recLen) > len(b) {
			break
		}
		if RecordType(b[off+8]) == 0 {
			break // invalid record type
		}
		lsn := int64(binary.BigEndian.Uint64(b[off : off+8]))
		if lsn <= 0 {
			break // LSNs are assigned from 1
		}
		lastLSN = lsn
		off += int(recLen)
	}
	return off, lastLSN
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
	recovered, lastSegmentID, err := recoverSegments(w.dir, fromLSN, filterHandler)
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
