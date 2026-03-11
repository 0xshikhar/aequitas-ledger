package unit

import (
	"bytes"
	"errors"
	"testing"

	"aequitas-ledger/internal/wal"
)

func TestRecordEncodeDecodeRoundTrip(t *testing.T) {
	in := wal.Record{Type: wal.RecordTypeTransfer, Payload: []byte("hello-ledger")}

	frame, err := wal.EncodeRecord(in)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	out, err := wal.DecodeRecord(frame)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if out.Type != in.Type {
		t.Fatalf("type mismatch: got=%v want=%v", out.Type, in.Type)
	}
	if !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("payload mismatch: got=%x want=%x", out.Payload, in.Payload)
	}
}

func TestRecordDecodeCorruption(t *testing.T) {
	in := wal.Record{Type: wal.RecordTypeTransfer, Payload: []byte("payload")}
	frame, err := wal.EncodeRecord(in)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	frame[len(frame)-1] ^= 0xFF
	_, err = wal.DecodeRecord(frame)
	if !errors.Is(err, wal.ErrCorrupted) {
		t.Fatalf("expected ErrCorrupted, got %v", err)
	}
}

func FuzzDecodeRecord_NoPanicOnRandomBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, err := wal.DecodeRecord(data)
		if err == nil {
			return
		}
		if !errors.Is(err, wal.ErrCorrupted) {
			t.Fatalf("unexpected error type: %v", err)
		}
	})
}
