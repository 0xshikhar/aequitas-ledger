package audit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/wal"
)

// buildHealthyWAL produces a WAL with two accounts, idempotency-keyed
// transfers across several batches, and (optionally) a follower-style
// duplicate account record.
func buildHealthyWAL(t *testing.T, dir string) {
	t.Helper()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cur := [4]byte{'U', 'S', 'D', 0}
	accA, accB := [16]byte{0xAA}, [16]byte{0xBB}
	acc := func(id [16]byte, credits uint64) wal.Record {
		return wal.Record{Type: wal.RecordTypeAccount, Payload: core.EncodeAccountPayload(core.Account{ID: id, Currency: cur, PostedCredits: core.Uint128{Lo: credits}})}
	}
	tr := func(id byte, key byte, from, to [16]byte, amt uint64) wal.Record {
		var k [32]byte
		k[0] = key
		return wal.Record{Type: wal.RecordTypeTransfer, Payload: core.EncodeTransferPayload(core.Transfer{
			ID: [16]byte{id}, DebitAccountID: from, CreditAccountID: to,
			Amount: core.Uint128{Lo: amt}, IdempotencyKey: k,
		})}
	}

	batches := [][]wal.Record{
		{acc(accA, 1000), acc(accB, 0)},
		{tr(1, 1, accA, accB, 10), tr(2, 2, accA, accB, 20)},
		{tr(3, 3, accA, accB, 30)},
	}
	for _, b := range batches {
		if _, err := w.AppendBatch(b); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestAuditHealthyWALPasses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	buildHealthyWAL(t, dir)

	rep, err := Audit(dir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !rep.Valid() {
		t.Fatalf("healthy WAL reported %d violations: %v", len(rep.Violations), rep.Violations)
	}
	if rep.Batches != 3 {
		t.Fatalf("batches = %d, want 3", rep.Batches)
	}
	if rep.Accounts != 2 || rep.Transfers != 3 {
		t.Fatalf("accounts=%d transfers=%d, want 2/3", rep.Accounts, rep.Transfers)
	}
	if rep.Segments != 1 {
		t.Fatalf("segments = %d, want 1", rep.Segments)
	}
}

func TestAuditDetectsCorruptedFrame(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	buildHealthyWAL(t, dir)

	// Flip one byte inside the first account's payload (after its header):
	// frame 0 ends at 81; corrupt byte 40.
	seg := filepath.Join(dir, "wal-000001.seg")
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	data[40] ^= 0xFF
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}

	rep, err := Audit(dir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Valid() {
		t.Fatal("corrupted frame not detected")
	}
	found := false
	for _, v := range rep.Violations {
		if v.Kind == ViolationCorruptFrame {
			found = true
		}
	}
	if !found {
		t.Fatalf("want corrupt_frame violation, got %v", rep.Violations)
	}
}

func TestAuditDetectsTornTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	buildHealthyWAL(t, dir)

	seg := filepath.Join(dir, "wal-000001.seg")
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	// Cut 10 bytes off the tail: the last commit marker is torn.
	if err := os.WriteFile(seg, data[:len(data)-10], 0o644); err != nil {
		t.Fatal(err)
	}

	rep, err := Audit(dir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.TornTail == nil {
		t.Fatalf("torn tail not reported: %v", rep.Violations)
	}
	// The committed batches before the tail must still audit clean of
	// conservation issues.
	for _, v := range rep.Violations {
		if v.Kind == ViolationConservation || v.Kind == ViolationBatchCount {
			t.Fatalf("pre-tail history misjudged: %v", v)
		}
	}
}

func TestAuditDetectsDuplicateIdempotencyKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	cur := [4]byte{'U', 'S', 'D', 0}
	accA, accB := [16]byte{0xAA}, [16]byte{0xBB}
	var key [32]byte
	key[0] = 7
	// Two DIFFERENT transfer IDs sharing one key, appended directly —
	// bypassing the engine's dedup is exactly what the auditor exists to
	// catch (e.g. a misbehaving upstream writer).
	recs := []wal.Record{
		{Type: wal.RecordTypeAccount, Payload: core.EncodeAccountPayload(core.Account{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 100}})},
		{Type: wal.RecordTypeAccount, Payload: core.EncodeAccountPayload(core.Account{ID: accB, Currency: cur})},
		{Type: wal.RecordTypeTransfer, Payload: core.EncodeTransferPayload(core.Transfer{
			ID: [16]byte{1}, DebitAccountID: accA, CreditAccountID: accB,
			Amount: core.Uint128{Lo: 5}, IdempotencyKey: key,
		})},
		{Type: wal.RecordTypeTransfer, Payload: core.EncodeTransferPayload(core.Transfer{
			ID: [16]byte{2}, DebitAccountID: accA, CreditAccountID: accB,
			Amount: core.Uint128{Lo: 5}, IdempotencyKey: key,
		})},
	}
	if _, err := w.AppendBatch(recs); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	rep, err := Audit(dir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Valid() {
		t.Fatal("duplicate idempotency key not detected")
	}
	found := false
	for _, v := range rep.Violations {
		if v.Kind == ViolationDuplicateKey && strings.Contains(v.Detail, "transfers") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want duplicate-key violation, got %v", rep.Violations)
	}
}

func TestAuditDetectsUnknownAccountTransfer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// A transfer whose debit account was never created.
	recs := []wal.Record{
		{Type: wal.RecordTypeTransfer, Payload: core.EncodeTransferPayload(core.Transfer{
			ID: [16]byte{1}, DebitAccountID: [16]byte{0xEE}, CreditAccountID: [16]byte{0xFF},
			Amount: core.Uint128{Lo: 5},
		})},
	}
	if _, err := w.AppendBatch(recs); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	rep, err := Audit(dir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	found := false
	for _, v := range rep.Violations {
		if v.Kind == ViolationConservation && strings.Contains(v.Detail, "unknown account") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want unknown-account conservation violation, got %v", rep.Violations)
	}
}
