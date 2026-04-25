package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	RecordHeaderSize = 13 // 8 bytes LSN + 1 byte type + 4 bytes payload length
	RecordCRCSize    = 4
)

var (
	ErrCorrupted          = errors.New("wal: corrupted record")
	ErrPayloadTooLarge    = errors.New("wal: payload too large")
	ErrInvalidRecordType  = errors.New("wal: invalid record type")
	ErrInvalidRecordFrame = errors.New("wal: invalid record frame")
)

type RecordType uint8

const (
	RecordTypeTransfer    RecordType = 1
	RecordTypeAccount     RecordType = 2
	RecordTypeCheckpoint  RecordType = 3
	RecordTypeBatchCommit RecordType = 4
	// RecordTypeHead is replication-stream-only (never written to the WAL):
	// a periodic announcement carrying the primary's durable head LSN so
	// followers can report true lag.
	RecordTypeHead RecordType = 5
)

type Record struct {
	LSN     uint64
	Type    RecordType
	Payload []byte
}

// EncodeRecord returns the wire frame for one record. Convenience wrapper
// around appendFrame — batch writers should append into a shared buffer
// instead (S2.1: one allocation per batch, not per record).
func EncodeRecord(r Record) ([]byte, error) {
	return appendFrame(nil, r)
}

// appendFrame appends the wire frame [LSN:8|type:1|len:4|payload|crc32:4] to
// dst without allocating per record.
func appendFrame(dst []byte, r Record) ([]byte, error) {
	if r.Type == 0 {
		return dst, ErrInvalidRecordType
	}
	if len(r.Payload) > int(^uint32(0)) {
		return dst, ErrPayloadTooLarge
	}
	start := len(dst)
	var hdr [RecordHeaderSize]byte
	binary.BigEndian.PutUint64(hdr[0:8], r.LSN)
	hdr[8] = byte(r.Type)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(r.Payload)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, r.Payload...)
	checksum := crc32.ChecksumIEEE(dst[start:])
	var crc [RecordCRCSize]byte
	binary.BigEndian.PutUint32(crc[:], checksum)
	return append(dst, crc[:]...), nil
}

// appendCommitFrame appends the BatchCommit marker that frames a batch
// atomically: payload is [commitLSN:8][recordCount:4].
func appendCommitFrame(dst []byte, commitLSN int64, count int) []byte {
	var payload [12]byte
	binary.BigEndian.PutUint64(payload[0:8], uint64(commitLSN))
	binary.BigEndian.PutUint32(payload[8:12], uint32(count))
	start := len(dst)
	var hdr [RecordHeaderSize]byte
	binary.BigEndian.PutUint64(hdr[0:8], uint64(commitLSN))
	hdr[8] = byte(RecordTypeBatchCommit)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(payload)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, payload[:]...)
	checksum := crc32.ChecksumIEEE(dst[start:])
	var crc [RecordCRCSize]byte
	binary.BigEndian.PutUint32(crc[:], checksum)
	return append(dst, crc[:]...)
}

func DecodeRecord(frame []byte) (Record, error) {
	if len(frame) < RecordHeaderSize+RecordCRCSize {
		return Record{}, ErrCorrupted
	}

	lsn := binary.BigEndian.Uint64(frame[0:8])
	rt := RecordType(frame[8])
	if rt == 0 {
		return Record{}, ErrCorrupted
	}

	payloadLen := int(binary.BigEndian.Uint32(frame[9:13]))
	expectedLen := RecordHeaderSize + payloadLen + RecordCRCSize
	if payloadLen < 0 || len(frame) != expectedLen {
		return Record{}, ErrCorrupted
	}

	calc := crc32.ChecksumIEEE(frame[:RecordHeaderSize+payloadLen])
	got := binary.BigEndian.Uint32(frame[RecordHeaderSize+payloadLen:])
	if calc != got {
		return Record{}, ErrCorrupted
	}

	payload := make([]byte, payloadLen)
	copy(payload, frame[RecordHeaderSize:RecordHeaderSize+payloadLen])

	return Record{LSN: lsn, Type: rt, Payload: payload}, nil
}
