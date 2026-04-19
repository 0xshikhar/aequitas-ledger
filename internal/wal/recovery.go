package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

func recoverSegments(dir string, fromLSN int64, handler func(Record) error) (recoveredLSN int64, lastSegmentID int, err error) {
	ids, err := listSegmentIDs(dir)
	if err != nil {
		return 0, 0, err
	}
	if len(ids) == 0 {
		return 0, 0, nil
	}

	for idx, id := range ids {
		path := segmentPath(dir, id)
		lastSegmentID = id

		// A loaded snapshot covers every record at or below fromLSN, so a
		// segment whose max LSN is <= fromLSN cannot contribute to the delta
		// replay. Only the newest segment can carry a torn tail (any earlier
		// crash tail was truncated by a previous recovery), so earlier segments
		// are safe to skip reading entirely.
		if fromLSN > 0 && idx < len(ids)-1 {
			if maxLSN, perr := peekSegmentMaxLSN(path); perr == nil && maxLSN <= fromLSN {
				continue
			}
		}

		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("open segment %s for recovery: %w", path, err)
		}

		stop, segLSN, truncOffset, rerr := replaySegment(f, handler)
		_ = f.Close()
		if rerr != nil {
			return 0, 0, rerr
		}
		if segLSN > recoveredLSN {
			recoveredLSN = segLSN
		}

		if stop {
			if err := truncateSegment(path, truncOffset); err != nil {
				return 0, 0, err
			}
			// Everything after first invalid/truncated tail is ignored.
			// Later segments (if any) are stale for this crash boundary.
			if idx < len(ids)-1 {
				for _, staleID := range ids[idx+1:] {
					if err := os.Remove(segmentPath(dir, staleID)); err != nil && !errors.Is(err, os.ErrNotExist) {
						return 0, 0, fmt.Errorf("remove stale segment %d: %w", staleID, err)
					}
				}
				lastSegmentID = id
			}
			break
		}
	}

	return recoveredLSN, lastSegmentID, nil
}

// replaySegment sequentially replays records from the segment.
// Returns:
// - stop=true if a truncated/corrupted tail is found
// - records replayed count
// - truncate offset when stop=true
func replaySegment(f *os.File, handler func(Record) error) (stop bool, highestLSN int64, truncateOffset int64, err error) {
	stat, err := f.Stat()
	if err != nil {
		return false, 0, 0, fmt.Errorf("stat segment: %w", err)
	}
	size := stat.Size()
	if size == 0 {
		return false, 0, 0, nil
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		return false, 0, 0, fmt.Errorf("read segment: %w", err)
	}

	offset := int64(0)
	lastCommittedOffset := int64(0)
	var pendingBatch []Record

	for offset < size {
		if size-offset < int64(RecordHeaderSize+RecordCRCSize) {
			return true, highestLSN, lastCommittedOffset, nil
		}

		payloadLen := int64(binary.BigEndian.Uint32(data[offset+9 : offset+13]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			return true, highestLSN, lastCommittedOffset, nil
		}

		raw := data[offset : offset+recLen]
		rec, err := DecodeRecord(raw)
		if err != nil {
			if errors.Is(err, ErrCorrupted) {
				return true, highestLSN, lastCommittedOffset, nil
			}
			return false, highestLSN, 0, err
		}

		if rec.Type == RecordTypeBatchCommit {
			var expectedCount uint32
			if len(rec.Payload) >= 12 {
				expectedCount = binary.BigEndian.Uint32(rec.Payload[8:12])
			}
			if len(pendingBatch) == int(expectedCount) {
				for _, pendingRec := range pendingBatch {
					if err := replayRecord(pendingRec, handler); err != nil {
						return false, highestLSN, 0, err
					}
				}
				highestLSN = int64(rec.LSN)
				lastCommittedOffset = offset + recLen
			}
			pendingBatch = pendingBatch[:0]
		} else {
			pendingBatch = append(pendingBatch, rec)
		}

		offset += recLen
	}

	// Any uncommitted batch at end of file gets truncated
	if len(pendingBatch) > 0 {
		return true, highestLSN, lastCommittedOffset, nil
	}

	return false, highestLSN, lastCommittedOffset, nil
}

// detectTruncation returns the last valid offset in a segment-like byte slice.
func detectTruncation(data []byte) (int64, error) {
	offset := int64(0)
	lastValid := int64(0)
	size := int64(len(data))
	for offset < size {
		if size-offset < int64(RecordHeaderSize+RecordCRCSize) {
			break
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[offset+9 : offset+13]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			break
		}
		rec, err := DecodeRecord(data[offset : offset+recLen])
		if err != nil {
			break
		}
		if rec.Type == RecordTypeBatchCommit {
			lastValid = offset + recLen
		}
		offset += recLen
	}
	return lastValid, nil
}

func replayRecord(rec Record, handler func(Record) error) error {
	if handler == nil {
		return nil
	}
	return handler(rec)
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
