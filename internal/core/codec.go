package core

import (
	"encoding/binary"
	"errors"
)

const (
	// TransferPayloadSize: 16+16+16+16+32+8(ts)+8(timeout)+4(flags) = 116.
	TransferPayloadSize = 116
	// AccountPayloadSize matches core.AccountStructSize (96): the two pending
	// balance fields joined the struct in D1.2 (snapshot format v2).
	AccountPayloadSize = 96
)

var (
	ErrInvalidTransferPayload = errors.New("core: invalid transfer payload")
	ErrInvalidAccountPayload  = errors.New("core: invalid account payload")
)

// EncodeTransferPayload returns a freshly allocated wire payload. Hot paths
// should use AppendTransferPayload into a shared buffer instead (S2.1).
func EncodeTransferPayload(t Transfer) []byte {
	return AppendTransferPayload(make([]byte, 0, TransferPayloadSize), t)
}

// AppendTransferPayload appends t's wire payload to dst without allocating
// per call.
func AppendTransferPayload(dst []byte, t Transfer) []byte {
	var zeros [TransferPayloadSize]byte
	start := len(dst)
	dst = append(dst, zeros[:]...)
	b := dst
	off := start
	copy(b[off:off+16], t.ID[:])
	off += 16
	copy(b[off:off+16], t.DebitAccountID[:])
	off += 16
	copy(b[off:off+16], t.CreditAccountID[:])
	off += 16
	binary.BigEndian.PutUint64(b[off:off+8], t.Amount.Hi)
	binary.BigEndian.PutUint64(b[off+8:off+16], t.Amount.Lo)
	off += 16
	copy(b[off:off+32], t.IdempotencyKey[:])
	off += 32
	binary.BigEndian.PutUint64(b[off:off+8], uint64(t.Timestamp))
	off += 8
	binary.BigEndian.PutUint64(b[off:off+8], t.Timeout)
	off += 8
	binary.BigEndian.PutUint32(b[off:off+4], t.Flags)
	return b
}

func DecodeTransferPayload(b []byte) (Transfer, error) {
	if len(b) != TransferPayloadSize {
		return Transfer{}, ErrInvalidTransferPayload
	}
	var t Transfer
	off := 0
	copy(t.ID[:], b[off:off+16])
	off += 16
	copy(t.DebitAccountID[:], b[off:off+16])
	off += 16
	copy(t.CreditAccountID[:], b[off:off+16])
	off += 16
	t.Amount = Uint128{
		Hi: binary.BigEndian.Uint64(b[off : off+8]),
		Lo: binary.BigEndian.Uint64(b[off+8 : off+16]),
	}
	off += 16
	copy(t.IdempotencyKey[:], b[off:off+32])
	off += 32
	t.Timestamp = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	t.Timeout = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	t.Flags = binary.BigEndian.Uint32(b[off : off+4])
	return t, nil
}

// EncodeAccountPayload returns a freshly allocated wire payload. Hot paths
// should use AppendAccountPayload into a shared buffer instead (S2.1).
func EncodeAccountPayload(a Account) []byte {
	return AppendAccountPayload(make([]byte, 0, AccountPayloadSize), a)
}

// AppendAccountPayload appends a's wire payload to dst without allocating
// per call.
func AppendAccountPayload(dst []byte, a Account) []byte {
	var zeros [AccountPayloadSize]byte
	start := len(dst)
	dst = append(dst, zeros[:]...)
	b := dst
	off := start
	copy(b[off:off+16], a.ID[:])
	off += 16
	copy(b[off:off+4], a.Currency[:])
	off += 4
	binary.BigEndian.PutUint64(b[off:off+8], a.PostedDebits.Hi)
	binary.BigEndian.PutUint64(b[off+8:off+16], a.PostedDebits.Lo)
	off += 16
	binary.BigEndian.PutUint64(b[off:off+8], a.PostedCredits.Hi)
	binary.BigEndian.PutUint64(b[off+8:off+16], a.PostedCredits.Lo)
	off += 16
	binary.BigEndian.PutUint64(b[off:off+8], a.PendingDebits.Hi)
	binary.BigEndian.PutUint64(b[off+8:off+16], a.PendingDebits.Lo)
	off += 16
	binary.BigEndian.PutUint64(b[off:off+8], a.PendingCredits.Hi)
	binary.BigEndian.PutUint64(b[off+8:off+16], a.PendingCredits.Lo)
	off += 16
	binary.BigEndian.PutUint32(b[off:off+4], a.Flags)
	return b
}

func DecodeAccountPayload(b []byte) (Account, error) {
	if len(b) != AccountPayloadSize {
		return Account{}, ErrInvalidAccountPayload
	}
	var a Account
	off := 0
	copy(a.ID[:], b[off:off+16])
	off += 16
	copy(a.Currency[:], b[off:off+4])
	off += 4
	a.PostedDebits = Uint128{
		Hi: binary.BigEndian.Uint64(b[off : off+8]),
		Lo: binary.BigEndian.Uint64(b[off+8 : off+16]),
	}
	off += 16
	a.PostedCredits = Uint128{
		Hi: binary.BigEndian.Uint64(b[off : off+8]),
		Lo: binary.BigEndian.Uint64(b[off+8 : off+16]),
	}
	off += 16
	a.PendingDebits = Uint128{
		Hi: binary.BigEndian.Uint64(b[off : off+8]),
		Lo: binary.BigEndian.Uint64(b[off+8 : off+16]),
	}
	off += 16
	a.PendingCredits = Uint128{
		Hi: binary.BigEndian.Uint64(b[off : off+8]),
		Lo: binary.BigEndian.Uint64(b[off+8 : off+16]),
	}
	off += 16
	a.Flags = binary.BigEndian.Uint32(b[off : off+4])
	return a, nil
}
