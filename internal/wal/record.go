package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	RecordHeaderSize = 5 // 1 byte type + 4 bytes payload length
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
	RecordTypeTransfer RecordType = 1
	RecordTypeAccount  RecordType = 2
)

type Record struct {
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
	out[0] = byte(r.Type)
	binary.BigEndian.PutUint32(out[1:5], uint32(len(r.Payload)))
	copy(out[5:5+len(r.Payload)], r.Payload)

	checksum := crc32.ChecksumIEEE(out[:5+len(r.Payload)])
	binary.BigEndian.PutUint32(out[5+len(r.Payload):], checksum)

	return out, nil
}

func DecodeRecord(frame []byte) (Record, error) {
	if len(frame) < RecordHeaderSize+RecordCRCSize {
		return Record{}, ErrCorrupted
	}

	rt := RecordType(frame[0])
	if rt == 0 {
		return Record{}, ErrCorrupted
	}

	payloadLen := int(binary.BigEndian.Uint32(frame[1:5]))
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

	return Record{Type: rt, Payload: payload}, nil
}
