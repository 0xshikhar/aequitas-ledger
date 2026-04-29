package core

import (
	"encoding/binary"
	"errors"
)

const (
	// TransferPayloadSize matches TransferStructSize = 144 bytes:
	// 16(ID) + 16(Debit) + 16(Credit) + 16(Amount) + 32(Idemp) + 16(UserData) + 8(TS) + 8(Timeout) + 4(Ledger) + 2(Code) + 2(pad) + 4(Flags) + 4(pad) = 144.
	TransferPayloadSize = 144
	// AccountPayloadSize matches AccountStructSize = 128 bytes:
	// 16(ID) + 4(Curr) + 4(Ledger) + 2(Code) + 2(pad) + 4(Flags) + 16(UserData) + 16(Debits) + 16(Credits) + 16(PendDebits) + 16(PendCredits) + 16(pad) = 128.
	AccountPayloadSize = 128
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
	copy(b[off:off+16], t.UserData128[:])
	off += 16
	binary.BigEndian.PutUint64(b[off:off+8], uint64(t.Timestamp))
	off += 8
	binary.BigEndian.PutUint64(b[off:off+8], t.Timeout)
	off += 8
	binary.BigEndian.PutUint32(b[off:off+4], t.Ledger)
	off += 4
	binary.BigEndian.PutUint16(b[off:off+2], t.Code)
	off += 2
	off += 2 // 2 bytes padding
	binary.BigEndian.PutUint32(b[off:off+4], t.Flags)
	off += 4
	// 4 bytes trailing padding already zeroed
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
	copy(t.UserData128[:], b[off:off+16])
	off += 16
	t.Timestamp = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	t.Timeout = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	t.Ledger = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	t.Code = binary.BigEndian.Uint16(b[off : off+2])
	off += 2
	off += 2 // skip 2 bytes padding
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
	binary.BigEndian.PutUint32(b[off:off+4], a.Ledger)
	off += 4
	binary.BigEndian.PutUint16(b[off:off+2], a.Code)
	off += 2
	off += 2 // 2 bytes padding
	binary.BigEndian.PutUint32(b[off:off+4], a.Flags)
	off += 4
	copy(b[off:off+16], a.UserData128[:])
	off += 16
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
	// 16 bytes reserved already zeroed
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
	a.Ledger = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	a.Code = binary.BigEndian.Uint16(b[off : off+2])
	off += 2
	off += 2 // skip 2 bytes padding
	a.Flags = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	copy(a.UserData128[:], b[off:off+16])
	off += 16
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
	return a, nil
}
