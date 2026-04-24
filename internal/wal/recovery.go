package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// replayState carries batch-atomicity state across segment boundaries. A batch
// may straddle a segment rotation — AppendBatch rotates when the next record
// does not fit, so a batch's records can land in one segment and its
// BatchCommit record in the next. Pending records and the highest committed
// LSN must therefore survive replaySegment calls; treating them per-segment
// would make a rotation-straddling batch look like a torn tail and silently
// drop a batch that was fsynced and acked to clients.
type replayState struct {
	pending    []Record
	highestLSN int64
}

func recoverSegments(dir string, fromLSN int64, handler func(Record) error) (recoveredLSN int64, lastSegmentID int, err error) {
	ids, err := listSegmentIDs(dir)
	if err != nil {
		return 0, 0, err
	}
	if len(ids) == 0 {
		return 0, 0, nil
	}

	st := &replayState{}
	type segProgress struct {
		id              int
		committedOffset int64
	}
	// processed tracks read segments so uncommitted tails can be truncated
	// from every segment a straddling batch touched, not just the last.
	var processed []segProgress
	stopIdx := -1

	for idx, id := range ids {
		path := segmentPath(dir, id)
		lastSegmentID = id

		// A loaded snapshot covers every record at or below fromLSN, so a
		// segment whose max LSN is <= fromLSN cannot contribute to the delta
		// replay. Only the newest segment can carry a torn tail (any earlier
		// crash tail was truncated by a previous recovery), so earlier
		// segments are safe to skip reading entirely. If such a segment held
		// records of a batch whose commit lies in a later read segment, the
		// commit's expected count will not match and that batch is dropped —
		// safe, because the batch is then entirely at or below fromLSN (LSNs
		// are contiguous and fromLSN is always a commit-record LSN, so it
		// cannot fall inside a batch's record range) and is therefore already
		// reflected in the snapshot.
		if fromLSN > 0 && idx < len(ids)-1 {
			if maxLSN, perr := peekSegmentMaxLSN(path); perr == nil && maxLSN <= fromLSN {
				continue
			}
		}

		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("open segment %s for recovery: %w", path, err)
		}

		stop, committedOffset, rerr := replaySegment(f, handler, st)
		_ = f.Close()
		if rerr != nil {
			return 0, 0, rerr
		}
		processed = append(processed, segProgress{id: id, committedOffset: committedOffset})

		if stop {
			stopIdx = idx
			break
		}
	}

	// Uncommitted state is either a corrupt frame (stopIdx >= 0) or an
	// uncommitted batch tail at the end of the WAL. Uncommitted records above
	// the caller's snapshot horizon are truncated from each read segment (a
	// no-op for segments that ended on a commit boundary), and on corruption
	// later segments are dropped as stale — their contents cannot be
	// frame-walked with confidence. Uncommitted records at or below fromLSN
	// are covered by the snapshot: leave them (and any committed data they
	// sit next to) untouched rather than shrinking the WAL below the snapshot
	// horizon; a later full recovery enforces the strict boundary.
	if stopIdx >= 0 || len(st.pending) > 0 {
		strict := fromLSN == 0 || minRecordLSN(st.pending) > fromLSN
		if strict {
			for _, seg := range processed {
				p := segmentPath(dir, seg.id)
				info, err := os.Stat(p)
				if err != nil || info.Size() == seg.committedOffset {
					continue
				}
				if err := truncateSegment(p, seg.committedOffset); err != nil {
					return 0, 0, err
				}
			}
			if stopIdx >= 0 {
				for _, staleID := range ids[stopIdx+1:] {
					if err := os.Remove(segmentPath(dir, staleID)); err != nil && !errors.Is(err, os.ErrNotExist) {
						return 0, 0, fmt.Errorf("remove stale segment %d: %w", staleID, err)
					}
				}
				lastSegmentID = ids[stopIdx]
			}
		}
	}

	return st.highestLSN, lastSegmentID, nil
}

// minRecordLSN returns the smallest LSN among the given records, or
// math.MaxInt64 when the slice is empty.
func minRecordLSN(records []Record) int64 {
	minLSN := int64(^uint64(0) >> 1)
	for _, r := range records {
		if int64(r.LSN) < minLSN {
			minLSN = int64(r.LSN)
		}
	}
	return minLSN
}

// replaySegment sequentially replays one segment's records into st. Batch
// state (st.pending) may be carried in from, and left dangling at the end of,
// this segment when a batch straddles the rotation boundary. Returns:
//   - stop=true when a truncated/corrupted frame makes the rest of the segment
//     (and later segments) untrustworthy; committedOffset then points just past
//     the last committed record
//   - committedOffset: offset just past the last committed record in this
//     segment (== size when the segment ends on a commit boundary)
func replaySegment(f *os.File, handler func(Record) error, st *replayState) (stop bool, committedOffset int64, err error) {
	stat, err := f.Stat()
	if err != nil {
		return false, 0, fmt.Errorf("stat segment: %w", err)
	}
	size := stat.Size()
	if size == 0 {
		return false, 0, nil
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		return false, 0, fmt.Errorf("read segment: %w", err)
	}

	offset := int64(0)
	committedOffset = int64(0)

	for offset < size {
		if size-offset < int64(RecordHeaderSize+RecordCRCSize) {
			return true, committedOffset, nil
		}

		payloadLen := int64(binary.BigEndian.Uint32(data[offset+9 : offset+13]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			return true, committedOffset, nil
		}

		raw := data[offset : offset+recLen]
		rec, err := DecodeRecord(raw)
		if err != nil {
			if errors.Is(err, ErrCorrupted) {
				return true, committedOffset, nil
			}
			return false, committedOffset, err
		}

		if rec.Type == RecordTypeBatchCommit {
			var expectedCount uint32
			if len(rec.Payload) >= 12 {
				expectedCount = binary.BigEndian.Uint32(rec.Payload[8:12])
			}
			if len(st.pending) == int(expectedCount) {
				for _, pendingRec := range st.pending {
					if err := handler(pendingRec); err != nil {
						return false, committedOffset, err
					}
				}
				st.highestLSN = int64(rec.LSN)
				committedOffset = offset + recLen
			}
			// A count mismatch discards the pending records: without their
			// matching commit they were never a committed batch.
			st.pending = st.pending[:0]
		} else {
			st.pending = append(st.pending, rec)
		}

		offset += recLen
	}

	return false, committedOffset, nil
}

func truncateSegment(path string, offset int64) error {
	if offset < 0 {
		offset = 0
	}
	if err := os.Truncate(path, offset); err != nil {
		return fmt.Errorf("truncate segment %s to %d: %w", path, offset, err)
	}
	return nil
}
