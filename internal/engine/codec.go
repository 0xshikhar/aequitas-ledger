package engine

import (
	"encoding/binary"
	"errors"

	"aequitas-ledger/internal/core"
)

const transferPayloadSize = 108

var errInvalidTransferPayload = errors.New("engine: invalid transfer payload")

func encodeTransferPayload(t core.Transfer) []byte {
	b := make([]byte, transferPayloadSize)
	off := 0
	copy(b[off:off+16], t.ID[:])
	off += 16
	copy(b[off:off+16], t.DebitAccountID[:])
	off += 16
	copy(b[off:off+16], t.CreditAccountID[:])
	off += 16
	copy(b[off:off+16], core.MarshalBinary(t.Amount))
	off += 16
	copy(b[off:off+32], t.IdempotencyKey[:])
	off += 32
	binary.BigEndian.PutUint64(b[off:off+8], uint64(t.Timestamp))
	off += 8
	binary.BigEndian.PutUint32(b[off:off+4], t.Flags)
	return b
}

func decodeTransferPayload(b []byte) (core.Transfer, error) {
	if len(b) != transferPayloadSize {
		return core.Transfer{}, errInvalidTransferPayload
	}
	var t core.Transfer
	off := 0
	copy(t.ID[:], b[off:off+16])
	off += 16
	copy(t.DebitAccountID[:], b[off:off+16])
	off += 16
	copy(t.CreditAccountID[:], b[off:off+16])
	off += 16
	amt, err := core.UnmarshalBinary(b[off : off+16])
	if err != nil {
		return core.Transfer{}, err
	}
	t.Amount = amt
	off += 16
	copy(t.IdempotencyKey[:], b[off:off+32])
	off += 32
	t.Timestamp = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	t.Flags = binary.BigEndian.Uint32(b[off : off+4])
	return t, nil
}
