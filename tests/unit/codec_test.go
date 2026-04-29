package unit_test

import (
	"bytes"
	"testing"
	"unsafe"

	"aequitas-ledger/internal/core"
)

func TestStructSizesAndPadding(t *testing.T) {
	if sz := unsafe.Sizeof(core.Account{}); sz != 128 {
		t.Fatalf("core.Account size = %d, expected 128", sz)
	}
	if sz := unsafe.Sizeof(core.Transfer{}); sz != 144 {
		t.Fatalf("core.Transfer size = %d, expected 144", sz)
	}
	if core.AccountPayloadSize != 128 {
		t.Fatalf("AccountPayloadSize = %d, expected 128", core.AccountPayloadSize)
	}
	if core.TransferPayloadSize != 144 {
		t.Fatalf("TransferPayloadSize = %d, expected 144", core.TransferPayloadSize)
	}
}

func TestAccountCodecRoundtrip(t *testing.T) {
	acc := core.Account{
		ID:             [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Currency:       [4]byte{'U', 'S', 'D', 0},
		Ledger:         42,
		Code:           101,
		Flags:          core.AccountFlagClosed,
		UserData128:    [16]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00},
		PostedDebits:   core.Uint128{Lo: 100, Hi: 0},
		PostedCredits:  core.Uint128{Lo: 500, Hi: 0},
		PendingDebits:  core.Uint128{Lo: 50, Hi: 0},
		PendingCredits: core.Uint128{Lo: 75, Hi: 0},
	}

	buf := core.AppendAccountPayload(nil, acc)
	if len(buf) != core.AccountPayloadSize {
		t.Fatalf("AppendAccountPayload produced %d bytes, expected %d", len(buf), core.AccountPayloadSize)
	}

	decoded, err := core.DecodeAccountPayload(buf)
	if err != nil {
		t.Fatalf("DecodeAccountPayload failed: %v", err)
	}

	if decoded.ID != acc.ID {
		t.Errorf("ID mismatch: got %v, want %v", decoded.ID, acc.ID)
	}
	if decoded.Currency != acc.Currency {
		t.Errorf("Currency mismatch: got %v, want %v", decoded.Currency, acc.Currency)
	}
	if decoded.Ledger != acc.Ledger {
		t.Errorf("Ledger mismatch: got %d, want %d", decoded.Ledger, acc.Ledger)
	}
	if decoded.Code != acc.Code {
		t.Errorf("Code mismatch: got %d, want %d", decoded.Code, acc.Code)
	}
	if decoded.Flags != acc.Flags {
		t.Errorf("Flags mismatch: got %d, want %d", decoded.Flags, acc.Flags)
	}
	if decoded.UserData128 != acc.UserData128 {
		t.Errorf("UserData128 mismatch: got %v, want %v", decoded.UserData128, acc.UserData128)
	}
	if decoded.PostedDebits != acc.PostedDebits || decoded.PostedCredits != acc.PostedCredits {
		t.Errorf("Posted balances mismatch")
	}
	if decoded.PendingDebits != acc.PendingDebits || decoded.PendingCredits != acc.PendingCredits {
		t.Errorf("Pending balances mismatch")
	}
}

func TestTransferCodecRoundtrip(t *testing.T) {
	tr := core.Transfer{
		ID:              [16]byte{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4},
		DebitAccountID:  [16]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		CreditAccountID: [16]byte{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45},
		Amount:          core.Uint128{Lo: 9999, Hi: 123},
		IdempotencyKey:  [32]byte{0xde, 0xad, 0xbe, 0xef},
		UserData128:     [16]byte{0xca, 0xfe, 0xba, 0xbe},
		Timestamp:       1700000000123456789,
		Timeout:         3000000000,
		Ledger:          999,
		Code:            777,
		Flags:           core.TransferFlagLinked | core.TransferFlagPending,
	}

	buf := core.AppendTransferPayload(nil, tr)
	if len(buf) != core.TransferPayloadSize {
		t.Fatalf("AppendTransferPayload produced %d bytes, expected %d", len(buf), core.TransferPayloadSize)
	}

	decoded, err := core.DecodeTransferPayload(buf)
	if err != nil {
		t.Fatalf("DecodeTransferPayload failed: %v", err)
	}

	if decoded.ID != tr.ID {
		t.Errorf("ID mismatch: got %v, want %v", decoded.ID, tr.ID)
	}
	if decoded.DebitAccountID != tr.DebitAccountID || decoded.CreditAccountID != tr.CreditAccountID {
		t.Errorf("Account IDs mismatch")
	}
	if decoded.Amount != tr.Amount {
		t.Errorf("Amount mismatch: got %v, want %v", decoded.Amount, tr.Amount)
	}
	if !bytes.Equal(decoded.IdempotencyKey[:], tr.IdempotencyKey[:]) {
		t.Errorf("IdempotencyKey mismatch")
	}
	if decoded.UserData128 != tr.UserData128 {
		t.Errorf("UserData128 mismatch: got %v, want %v", decoded.UserData128, tr.UserData128)
	}
	if decoded.Timestamp != tr.Timestamp {
		t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, tr.Timestamp)
	}
	if decoded.Timeout != tr.Timeout {
		t.Errorf("Timeout mismatch: got %d, want %d", decoded.Timeout, tr.Timeout)
	}
	if decoded.Ledger != tr.Ledger {
		t.Errorf("Ledger mismatch: got %d, want %d", decoded.Ledger, tr.Ledger)
	}
	if decoded.Code != tr.Code {
		t.Errorf("Code mismatch: got %d, want %d", decoded.Code, tr.Code)
	}
	if decoded.Flags != tr.Flags {
		t.Errorf("Flags mismatch: got %d, want %d", decoded.Flags, tr.Flags)
	}
}

func BenchmarkCodecZeroAlloc(b *testing.B) {
	acc := core.Account{
		ID:          [16]byte{1},
		Currency:    [4]byte{'U', 'S', 'D'},
		Ledger:      1,
		Code:        100,
		UserData128: [16]byte{2},
	}
	tr := core.Transfer{
		ID:          [16]byte{1},
		Amount:      core.Uint128{Lo: 100},
		Ledger:      1,
		Code:        100,
		UserData128: [16]byte{3},
		Flags:       core.TransferFlagLinked,
	}

	accBuf := make([]byte, 0, core.AccountPayloadSize)
	trBuf := make([]byte, 0, core.TransferPayloadSize)

	b.Run("AccountAppend", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = core.AppendAccountPayload(accBuf[:0], acc)
		}
	})

	b.Run("TransferAppend", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = core.AppendTransferPayload(trBuf[:0], tr)
		}
	})
}
