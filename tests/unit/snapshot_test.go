package unit

import (
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/snapshot"
)

func TestSnapshotWriteReadRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "snap-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot-00000000000000000100.snap")
	var lsn int64 = 100

	accs := []core.Account{
		{
			ID:            [16]byte{1},
			Currency:      [4]byte{'U', 'S', 'D'},
			PostedDebits:  core.FromUint64(50),
			PostedCredits: core.FromUint64(200),
		},
		{
			ID:            [16]byte{2},
			Currency:      [4]byte{'U', 'S', 'D'},
			PostedDebits:  core.FromUint64(100),
			PostedCredits: core.FromUint64(100),
		},
	}

	if err := snapshot.Write(snapPath, lsn, accs); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}

	readAccs, readLSN, err := snapshot.Read(snapPath)
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}

	if readLSN != lsn {
		t.Errorf("expected LSN %d, got %d", lsn, readLSN)
	}
	if len(readAccs) != len(accs) {
		t.Fatalf("expected %d accounts, got %d", len(accs), len(readAccs))
	}
	if readAccs[0].ID != accs[0].ID || readAccs[0].PostedCredits.Lo != 200 {
		t.Errorf("account 0 data mismatch: %+v", readAccs[0])
	}
}

func TestSnapshotChecksumCorruption(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "snap-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot-00000000000000000050.snap")
	accs := []core.Account{
		{ID: [16]byte{1}, Currency: [4]byte{'U', 'S', 'D'}},
	}
	if err := snapshot.Write(snapPath, 50, accs); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}

	// Corrupt a byte in the middle of the file
	data, _ := os.ReadFile(snapPath)
	data[10] ^= 0xFF
	_ = os.WriteFile(snapPath, data, 0644)

	_, _, err = snapshot.Read(snapPath)
	if err == nil {
		t.Errorf("expected CRC checksum mismatch error, got nil")
	}
}

func TestSnapshotLatest(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "snap-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	accs := []core.Account{{ID: [16]byte{1}}}

	_ = snapshot.Write(filepath.Join(tmpDir, "snapshot-00000000000000000010.snap"), 10, accs)
	_ = snapshot.Write(filepath.Join(tmpDir, "snapshot-00000000000000000050.snap"), 50, accs)
	_ = snapshot.Write(filepath.Join(tmpDir, "snapshot-00000000000000000030.snap"), 30, accs)

	latestPath, maxLSN, err := snapshot.Latest(tmpDir)
	if err != nil {
		t.Fatalf("failed to find latest snapshot: %v", err)
	}
	if maxLSN != 50 {
		t.Errorf("expected max LSN 50, got %d", maxLSN)
	}
	if filepath.Base(latestPath) != "snapshot-00000000000000000050.snap" {
		t.Errorf("unexpected latest path: %s", latestPath)
	}
}
