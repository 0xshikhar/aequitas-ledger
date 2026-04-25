package core

import (
	"encoding/binary"
	"errors"
)

const (
	TransferPayloadSize = 108
	AccountPayloadSize  = 64
)

var (
	ErrInvalidTransferPayload = errors.New("core: invalid transfer payload")
	ErrInvalidAccountPayload  = errors.New("core: invalid account payload")
)

func EncodeTransferPayload(t Transfer) []byte {
	b := make([]byte, TransferPayloadSize)
	off := 0
	copy(b[off:off+16], t.ID[:])
	off += 16
	copy(b[off:off+16], t.DebitAccountID[:])
	off += 16
	copy(b[off:off+16], t.CreditAccountID[:])
	off += 16
	copy(b[off:off+16], MarshalBinary(t.Amount))
	off += 16
	copy(b[off:off+32], t.IdempotencyKey[:])
	off += 32
	binary.BigEndian.PutUint64(b[off:off+8], uint64(t.Timestamp))
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
	amt, err := UnmarshalBinary(b[off : off+16])
	if err != nil {
		return Transfer{}, err
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

func EncodeAccountPayload(a Account) []byte {
	b := make([]byte, AccountPayloadSize)
	off := 0
	copy(b[off:off+16], a.ID[:])
	off += 16
	copy(b[off:off+4], a.Currency[:])
	off += 4
	copy(b[off:off+16], MarshalBinary(a.PostedDebits))
	off += 16
	copy(b[off:off+16], MarshalBinary(a.PostedCredits))
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
	debits, err := UnmarshalBinary(b[off : off+16])
	if err != nil {
		return Account{}, err
	}
	a.PostedDebits = debits
	off += 16
	credits, err := UnmarshalBinary(b[off : off+16])
	if err != nil {
		return Account{}, err
	}
	a.PostedCredits = credits
	off += 16
	a.Flags = binary.BigEndian.Uint32(b[off : off+4])
	return a, nil
}
