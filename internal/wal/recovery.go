package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

func recoverSegments(dir string, handler func(Record) error) (recoveredLSN int64, lastSegmentID int, err error) {
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

		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("open segment %s for recovery: %w", path, err)
		}

		stop, recs, truncOffset, rerr := replaySegment(f, handler)
		_ = f.Close()
		if rerr != nil {
			return 0, 0, rerr
		}
		recoveredLSN += recs

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
func replaySegment(f *os.File, handler func(Record) error) (stop bool, records int64, truncateOffset int64, err error) {
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
	for offset < size {
		if size-offset < RecordHeaderSize+RecordCRCSize {
			return true, records, offset, nil
		}

		payloadLen := int64(binary.BigEndian.Uint32(data[offset+1 : offset+5]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			return true, records, offset, nil
		}

		raw := data[offset : offset+recLen]
		rec, err := DecodeRecord(raw)
		if err != nil {
			if errors.Is(err, ErrCorrupted) {
				lastValid, derr := detectTruncation(data)
				if derr != nil {
					return false, 0, 0, derr
				}
				return true, records, lastValid, nil
			}
			return false, records, 0, err
		}

		if err := replayRecord(rec, handler); err != nil {
			return false, records, 0, err
		}
		records++
		offset += recLen
	}

	return false, records, 0, nil
}

// detectTruncation returns the last valid offset in a segment-like byte slice.
func detectTruncation(data []byte) (int64, error) {
	type span struct{ start, end int64 }
	spans := make([]span, 0, 64)
	offset := int64(0)
	size := int64(len(data))
	for offset < size {
		if size-offset < RecordHeaderSize+RecordCRCSize {
			break
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[offset+1 : offset+5]))
		recLen := int64(RecordHeaderSize) + payloadLen + int64(RecordCRCSize)
		if payloadLen < 0 || recLen <= 0 || offset+recLen > size {
			break
		}
		if _, err := DecodeRecord(data[offset : offset+recLen]); err != nil {
			break
		}
		spans = append(spans, span{start: offset, end: offset + recLen})
		offset += recLen
	}
	if len(spans) == 0 {
		return 0, nil
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	return spans[len(spans)-1].end, nil
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
