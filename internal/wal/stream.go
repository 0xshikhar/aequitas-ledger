package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// StreamReader incrementally streams committed records from the WAL starting
// after fromLSN, without loading whole segments into memory. It only reads
// the durable region: closed segments were fsynced at rotation, and the
// current segment is read strictly up to its synced offset — so a record is
// delivered only after the primary cannot lose it in a crash.
//
// Batching mirrors recovery: data records accumulate in pending and are
// released when their matching BatchCommit frame arrives. Batches whose
// expected count does not match the accumulated pending (possible only at
// stream start, when positioning skips segments entirely at or below fromLSN)
// are dropped — safe for the same reason as in recovery: fromLSN is always a
// commit-record LSN, so it cannot fall inside a batch's record range, and any
// dropped batch is entirely at or below the caller's last applied LSN.
//
// Next returns ok=false when the stream is caught up to the durable head;
// the cursor advances permanently, so the caller may sleep and call again.
// A StreamReader is not safe for concurrent use.
type StreamReader struct {
	w       *WAL
	fromLSN int64

	ids    []int // segment ids to read, in order
	idx    int   // index into ids
	maxID  int   // highest segment id seen (for re-listing)
	f      *os.File
	id     int   // current segment id
	size   int64 // current segment file size
	offset int64 // read offset within current segment

	pending []Record
	emit    []Record
}

func (w *WAL) NewStreamReader(fromLSN int64) *StreamReader {
	return &StreamReader{w: w, fromLSN: fromLSN}
}

// Close releases the streamer's file handles.
func (sr *StreamReader) Close() {
	if sr.f != nil {
		_ = sr.f.Close()
		sr.f = nil
	}
}

// Next returns the next record from a committed batch above fromLSN.
func (sr *StreamReader) Next() (Record, bool, error) {
	for {
		if len(sr.emit) > 0 {
			rec := sr.emit[0]
			sr.emit = sr.emit[1:]
			return rec, true, nil
		}

		opened, err := sr.ensureOpen()
		if err != nil {
			return Record{}, false, err
		}
		if !opened {
			return Record{}, false, nil // caught up: no eligible segments
		}

		limit, isCurrent, err := sr.durableLimit()
		if err != nil {
			return Record{}, false, err
		}
		// At the bound of the readable region?
		if sr.offset >= limit {
			if !isCurrent {
				// Closed segment fully consumed: move to the next one.
				if err := sr.advance(); err != nil {
					return Record{}, false, err
				}
				continue
			}
			// The live segment's durable end is here, but the segment itself
			// keeps growing: stay positioned and let the caller poll again.
			return Record{}, false, nil
		}
		if sr.offset+RecordHeaderSize+RecordCRCSize > limit {
			// A full header does not fit in the readable region.
			if !isCurrent {
				return Record{}, false, ErrCorrupted // closed segments are complete
			}
			return Record{}, false, nil // live tail: poll again later
		}

		rec, frameLen, caughtUp, err := sr.readFrame(limit, isCurrent)
		if err != nil {
			return Record{}, false, err
		}
		if caughtUp {
			// The frame crosses the live segment's synced bound: stay
			// positioned and poll again — do NOT advance.
			return Record{}, false, nil
		}
		sr.offset += frameLen

		if rec.Type == RecordTypeBatchCommit {
			var expectedCount uint32
			if len(rec.Payload) >= 12 {
				expectedCount = binary.BigEndian.Uint32(rec.Payload[8:12])
			}
			if len(sr.pending) == int(expectedCount) {
				sr.emit = append(sr.emit[:0], sr.pending...)
				sr.emit = append(sr.emit, rec)
				sr.pending = sr.pending[:0]
				first := sr.emit[0]
				sr.emit = sr.emit[1:]
				return first, true, nil
			}
			sr.pending = sr.pending[:0]
			continue
		}
		sr.pending = append(sr.pending, rec)
	}
}

// ensureOpen positions the reader on the next segment to read. It returns
// false when no eligible segment is available (caught up at segment level).
func (sr *StreamReader) ensureOpen() (bool, error) {
	if sr.f != nil {
		return true, nil
	}
	if sr.ids == nil {
		ids, err := listSegmentIDs(sr.w.dir)
		if err != nil {
			return false, err
		}
		if len(ids) == 0 {
			return false, nil
		}
		// Skip segments entirely at or below fromLSN (tail peek, no full
		// read). Segments whose max LSN cannot be determined are kept: the
		// per-frame LSN handling stays correct regardless.
		for _, id := range ids {
			if maxLSN, err := peekSegmentMaxLSN(segmentPath(sr.w.dir, id)); err == nil && maxLSN <= sr.fromLSN {
				continue
			}
			sr.ids = append(sr.ids, id)
		}
		if len(sr.ids) == 0 {
			return false, nil
		}
		sr.maxID = ids[len(ids)-1]
		sr.idx = 0
	}

	for sr.idx < len(sr.ids) {
		id := sr.ids[sr.idx]
		f, err := os.Open(segmentPath(sr.w.dir, id))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				sr.idx++ // truncated away since listing; try the next
				continue
			}
			return false, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return false, err
		}
		sr.f = f
		sr.id = id
		sr.size = info.Size()
		sr.offset = 0
		return true, nil
	}

	// All listed segments consumed: re-list for newly created ones.
	ids, err := listSegmentIDs(sr.w.dir)
	if err != nil {
		return false, err
	}
	appended := false
	for _, id := range ids {
		if id > sr.maxID {
			sr.ids = append(sr.ids, id)
			sr.maxID = id
			appended = true
		}
	}
	if !appended {
		return false, nil // caught up at segment level
	}
	return sr.ensureOpen()
}

// advance closes the current segment and moves to the next one.
func (sr *StreamReader) advance() error {
	if sr.f != nil {
		_ = sr.f.Close()
		sr.f = nil
	}
	sr.idx++
	_, err := sr.ensureOpen()
	return err
}

// durableLimit returns the readable byte bound for the reader's current
// segment and whether that segment is the WAL's live one.
func (sr *StreamReader) durableLimit() (limit int64, isCurrent bool, err error) {
	curID, syncedOffset := sr.w.Head()
	if sr.id == curID {
		// The live segment is durable only through its synced offset.
		return syncedOffset, true, nil
	}
	// Closed segments are fully durable (Rotate fsyncs before closing).
	return sr.size, false, nil
}

// readFrame decodes one frame at the current offset. caughtUp=true means the
// frame crosses the live segment's synced bound — the caller must stay
// positioned and poll again. An error within the durable bound of a closed
// segment is real corruption.
func (sr *StreamReader) readFrame(limit int64, isCurrent bool) (rec Record, frameLen int64, caughtUp bool, err error) {
	hdr := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(io.NewSectionReader(sr.f, sr.offset, RecordHeaderSize), hdr); err != nil {
		return Record{}, 0, false, err
	}
	payloadLen := int64(binary.BigEndian.Uint32(hdr[9:13]))
	if payloadLen < 0 || payloadLen > 1<<20 {
		return Record{}, 0, false, ErrCorrupted
	}
	frameLen = RecordHeaderSize + payloadLen + RecordCRCSize
	if sr.offset+frameLen > limit {
		if isCurrent {
			// Header fits but the full frame crosses the synced bound: the
			// tail write has not been fsynced yet. Caught up.
			return Record{}, 0, true, nil
		}
		// A closed segment is fully durable and should never truncate
		// mid-frame.
		return Record{}, 0, false, ErrCorrupted
	}

	frame := make([]byte, frameLen)
	if _, err := io.ReadFull(io.NewSectionReader(sr.f, sr.offset, frameLen), frame); err != nil {
		return Record{}, 0, false, err
	}
	rec, err = DecodeRecord(frame)
	if err != nil {
		return Record{}, 0, false, err // CRC failure within the durable bound: real corruption
	}
	return rec, frameLen, false, nil
}
