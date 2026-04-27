package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func setupTestEngineForFault(t *testing.T, dir string) (*engine.Ledger, [16]byte, [16]byte) {
	w, err := wal.Open(filepath.Join(dir, "wal"), 64<<20)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = filepath.Join(dir, "snapshots")
	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("failed to create ledger: %v", err)
	}

	ctx := context.Background()
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	if _, err := ledger.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 10000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	return ledger, accA, accB
}

func TestFaultInjection_BitFlips(t *testing.T) {
	injector := wal.NewFaultInjector()
	flipLocations := []string{"header", "payload", "crc"}

	for _, loc := range flipLocations {
		t.Run("FlipIn_"+loc, func(t *testing.T) {
			dir := t.TempDir()
			ledger, accA, accB := setupTestEngineForFault(t, dir)
			ctx := context.Background()

			// Batch 1: Transfer 500
			tr1 := core.Transfer{
				ID:              mkID(10),
				DebitAccountID:  accA,
				CreditAccountID: accB,
				Amount:          core.Uint128{Lo: 500},
			}
			if _, err := ledger.CreateTransfers(ctx, []core.Transfer{tr1}); err != nil {
				t.Fatalf("create transfer 1: %v", err)
			}

			// Find exact byte offset of the end of Batch 1
			segPath := filepath.Join(dir, "wal", "wal-000001.seg")
			infoBefore, err := os.Stat(segPath)
			if err != nil {
				t.Fatalf("stat segment: %v", err)
			}
			batch2Offset := infoBefore.Size()

			// Batch 2: Transfer 300
			tr2 := core.Transfer{
				ID:              mkID(11),
				DebitAccountID:  accA,
				CreditAccountID: accB,
				Amount:          core.Uint128{Lo: 300},
			}
			if _, err := ledger.CreateTransfers(ctx, []core.Transfer{tr2}); err != nil {
				t.Fatalf("create transfer 2: %v", err)
			}

			if err := ledger.Close(); err != nil {
				t.Fatalf("close ledger: %v", err)
			}

			// Determine target offset within Batch 2
			// Frame layout: [LSN: 8][Type: 1][PayloadLen: 4][Payload: 112][CRC: 4]
			var targetOffset int64
			switch loc {
			case "header":
				targetOffset = batch2Offset + 8 // RecordType byte
			case "payload":
				targetOffset = batch2Offset + 13 + 10 // Inside transfer payload
			case "crc":
				targetOffset = batch2Offset + 13 + 112 + 1 // Inside CRC32 trailer
			}

			// Inject bit flip
			if err := injector.FlipBit(segPath, targetOffset, 1); err != nil {
				t.Fatalf("inject bit flip failed: %v", err)
			}

			// Reopen ledger and recover
			wRecover, err := wal.Open(filepath.Join(dir, "wal"), 64<<20)
			if err != nil {
				t.Fatalf("reopen wal failed: %v", err)
			}
			cfg := engine.DefaultConfig()
			cfg.SnapshotDir = filepath.Join(dir, "snapshots")
			recoveredLedger, err := engine.NewLedger(cfg, wRecover)
			if err != nil {
				t.Fatalf("recovery failed with unexpected fatal error: %v", err)
			}
			defer recoveredLedger.Close()

			// Batch 1 must be recovered intact! Batch 2 must be halted due to corruption.
			acc, err := recoveredLedger.GetAccount(ctx, accA)
			if err != nil {
				t.Fatalf("get accA failed: %v", err)
			}

			// AccA should reflect batch 1 (500 debited), NOT batch 2 (300)
			expectedDebit := core.Uint128{Lo: 500}
			if acc.PostedDebits != expectedDebit {
				t.Fatalf("expected debits %v, got %v", expectedDebit, acc.PostedDebits)
			}

			accBRes, err := recoveredLedger.GetAccount(ctx, accB)
			if err != nil {
				t.Fatalf("get accB failed: %v", err)
			}
			expectedCredit := core.Uint128{Lo: 500}
			if accBRes.PostedCredits != expectedCredit {
				t.Fatalf("expected credits %v, got %v", expectedCredit, accBRes.PostedCredits)
			}
		})
	}
}

func TestFaultInjection_TornWriteAndGarbageTail(t *testing.T) {
	injector := wal.NewFaultInjector()

	dir := t.TempDir()
	ledger, accA, accB := setupTestEngineForFault(t, dir)
	ctx := context.Background()

	// Commit valid batch
	tr := core.Transfer{
		ID:              mkID(20),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 750},
	}
	if _, err := ledger.CreateTransfers(ctx, []core.Transfer{tr}); err != nil {
		t.Fatalf("create transfer failed: %v", err)
	}

	segPath := filepath.Join(dir, "wal", "wal-000001.seg")
	infoBefore, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	validCommitOffset := infoBefore.Size()

	if err := ledger.Close(); err != nil {
		t.Fatalf("close ledger failed: %v", err)
	}

	// Append a torn/partial frame header followed by immediate truncation
	garbage := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x99, // LSN 153
		0x01,                   // Type Transfer
		0x00, 0x00, 0x05, 0x00, // Payload length 1280 bytes
		0xDE, 0xAD, 0xBE, 0xEF, // Only 4 bytes of payload (torn write!)
	}
	if err := injector.CorruptBytes(segPath, validCommitOffset, garbage); err != nil {
		t.Fatalf("append torn write failed: %v", err)
	}
	if err := injector.TornWrite(segPath, validCommitOffset+int64(len(garbage))); err != nil {
		t.Fatalf("torn write truncation failed: %v", err)
	}

	// Reopen ledger and recover
	wRecover, err := wal.Open(filepath.Join(dir, "wal"), 64<<20)
	if err != nil {
		t.Fatalf("reopen wal failed: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = filepath.Join(dir, "snapshots")
	recoveredLedger, err := engine.NewLedger(cfg, wRecover)
	if err != nil {
		t.Fatalf("recovery failed with unexpected error: %v", err)
	}
	defer recoveredLedger.Close()

	// Prior committed batch must be intact
	acc, err := recoveredLedger.GetAccount(ctx, accA)
	if err != nil {
		t.Fatalf("get accA failed: %v", err)
	}
	if acc.PostedDebits != (core.Uint128{Lo: 750}) {
		t.Fatalf("expected debits 750, got %v", acc.PostedDebits)
	}
}
