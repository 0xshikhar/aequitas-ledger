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

func EncodeRecord(r Record) ([]byte, error) {
	if r.Type == 0 {
		return nil, ErrInvalidRecordType
	}
	if len(r.Payload) > int(^uint32(0)) {
		return nil, ErrPayloadTooLarge
	}

	frameLen := RecordHeaderSize + len(r.Payload) + RecordCRCSize
	out := make([]byte, frameLen)
	binary.BigEndian.PutUint64(out[0:8], r.LSN)
	out[8] = byte(r.Type)
	binary.BigEndian.PutUint32(out[9:13], uint32(len(r.Payload)))
	copy(out[13:13+len(r.Payload)], r.Payload)

	checksum := crc32.ChecksumIEEE(out[:13+len(r.Payload)])
	binary.BigEndian.PutUint32(out[13+len(r.Payload):], checksum)

	return out, nil
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
