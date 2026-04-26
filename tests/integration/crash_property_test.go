package integration

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// T3.1 — the crash-at-every-byte-offset property test.
//
// Recovery semantics have been rewritten three times (snapshot skipping,
// tail peeks, cross-segment batch state); point-scenario tests validated
// specific positions. This test sweeps the ENTIRE space: for a deterministic
// workload it truncates the WAL at every single byte offset and asserts the
// same property the C0.17 bug violated —
//
//	for every truncation offset, recovery applies exactly the batches whose
//	commit marker fully preceded the truncation point, never a partial batch.
//
// The verification uses an independent model: a minimal frame parser written
// inside the test (not the wal package) defines batch boundaries, and a
// map-based simulator recomputes balances. Neither shares code with the
// engine's recovery, so a shared-blind-spot bug cannot hide.

type t3Record struct {
	lsn     uint64
	typ     wal.RecordType
	payload []byte
}

type t3Batch struct {
	records   []t3Record
	commitLSN uint64
	endOffset int64 // byte offset just past the batch's commit frame
}

// parseWALFrames is the test's own frame walker — deliberately independent
// of internal/wal's recovery code.
func parseWALFrames(t *testing.T, data []byte) ([]t3Record, []t3Batch) {
	t.Helper()
	var records []t3Record
	var batches []t3Batch
	var pending []t3Record
	off := 0
	for off+13 <= len(data) {
		lsn := binary.BigEndian.Uint64(data[off : off+8])
		typ := wal.RecordType(data[off+8])
		plen := int(binary.BigEndian.Uint32(data[off+9 : off+13]))
		frameLen := 13 + plen + 4
		if off+frameLen > len(data) {
			break // torn tail in the pristine file would be a workload bug
		}
		frame := data[off : off+frameLen]
		if crc32.ChecksumIEEE(frame[:13+plen]) != binary.BigEndian.Uint32(frame[13+plen:]) {
			t.Fatalf("pristine workload has a corrupt frame at %d", off)
		}
		payload := make([]byte, plen)
		copy(payload, frame[13:13+plen])
		rec := t3Record{lsn: lsn, typ: typ, payload: payload}
		off += frameLen

		if typ == wal.RecordTypeBatchCommit {
			batches = append(batches, t3Batch{
				records:   append([]t3Record(nil), pending...), // copy: pending's array is reused
				commitLSN: lsn,
				endOffset: int64(off),
			})
			pending = pending[:0]
		} else {
			pending = append(pending, rec)
		}
		records = append(records, rec)
	}
	if len(pending) > 0 {
		t.Fatalf("pristine workload ends with an uncommitted batch")
	}
	return records, batches
}

// t3Model independently simulates balances from record payloads.
type t3Model struct {
	credits map[[16]byte]uint64
	debits  map[[16]byte]uint64
}

func newT3Model() *t3Model {
	return &t3Model{credits: map[[16]byte]uint64{}, debits: map[[16]byte]uint64{}}
}

func (m *t3Model) apply(rec t3Record) error {
	switch rec.typ {
	case wal.RecordTypeAccount:
		acc, err := core.DecodeAccountPayload(rec.payload)
		if err != nil {
			return err
		}
		m.credits[acc.ID] += acc.PostedCredits.Lo
		m.debits[acc.ID] += acc.PostedDebits.Lo
	case wal.RecordTypeTransfer:
		tr, err := core.DecodeTransferPayload(rec.payload)
		if err != nil {
			return err
		}
		m.debits[tr.DebitAccountID] += tr.Amount.Lo
		m.credits[tr.CreditAccountID] += tr.Amount.Lo
	}
	return nil
}

func (m *t3Model) checkInvariants(t *testing.T) {
	t.Helper()
	for id, c := range m.credits {
		if m.debits[id] > c {
			t.Fatalf("model invariant violated for %x: debits %d > credits %d", id, m.debits[id], c)
		}
	}
}

func TestCrashAtEveryByteOffset(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	// Deterministic workload: accounts first, then three multi-record
	// transfer batches — small enough to sweep every byte offset.
	w, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	cur := [4]byte{'U', 'S', 'D', 0}
	accA, accB := [16]byte{0xAA}, [16]byte{0xBB}
	mk := func(ty wal.RecordType, payload []byte) wal.Record {
		return wal.Record{Type: ty, Payload: payload}
	}
	if _, err := w.AppendBatch([]wal.Record{
		mk(wal.RecordTypeAccount, core.EncodeAccountPayload(core.Account{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 1000}})),
		mk(wal.RecordTypeAccount, core.EncodeAccountPayload(core.Account{ID: accB, Currency: cur})),
	}); err != nil {
		t.Fatalf("append accounts: %v", err)
	}
	tr := func(id byte, from, to [16]byte, amt uint64) wal.Record {
		return mk(wal.RecordTypeTransfer, core.EncodeTransferPayload(core.Transfer{
			ID: [16]byte{id}, DebitAccountID: from, CreditAccountID: to, Amount: core.Uint128{Lo: amt},
		}))
	}
	if _, err := w.AppendBatch([]wal.Record{
		tr(1, accA, accB, 10), tr(2, accA, accB, 20), tr(3, accA, accB, 30),
	}); err != nil {
		t.Fatalf("append batch 2: %v", err)
	}
	if _, err := w.AppendBatch([]wal.Record{
		tr(4, accA, accB, 40), tr(5, accA, accB, 50),
	}); err != nil {
		t.Fatalf("append batch 3: %v", err)
	}
	if _, err := w.AppendBatch([]wal.Record{
		tr(6, accA, accB, 60),
	}); err != nil {
		t.Fatalf("append batch 4: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segPath := filepath.Join(walDir, "wal-000001.seg")
	pristine, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read pristine: %v", err)
	}
	_, pristineBatches := parseWALFrames(t, pristine)

	// Sweep every byte offset.
	for offset := int64(0); offset <= int64(len(pristine)); offset++ {
		func() {
			caseDir := filepath.Join(dir, "case")
			_ = os.RemoveAll(caseDir)
			if err := os.MkdirAll(caseDir, 0o755); err != nil {
				t.Fatal(err)
			}
			truncated := make([]byte, offset)
			copy(truncated, pristine[:offset])
			if err := os.WriteFile(filepath.Join(caseDir, "wal-000001.seg"), truncated, 0o644); err != nil {
				t.Fatal(err)
			}

			// Independent model: batches whose commit fully preceded the
			// truncation point are applied; nothing else.
			model := newT3Model()
			want := 0
			wantLSN := uint64(0)
			for _, b := range pristineBatches {
				if b.endOffset <= offset {
					for _, r := range b.records {
						if err := model.apply(r); err != nil {
							t.Fatalf("offset %d: model apply: %v", offset, err)
						}
						want++
					}
					wantLSN = b.commitLSN
				}
			}
			model.checkInvariants(t)

			// Engine recovery over the truncated WAL.
			w2, err := wal.Open(caseDir, 1<<20)
			if err != nil {
				t.Fatalf("offset %d: open: %v", offset, err)
			}
			var got []t3Record
			err = w2.Recover(func(r wal.Record) error {
				got = append(got, t3Record{lsn: r.LSN, typ: r.Type, payload: append([]byte(nil), r.Payload...)})
				return nil
			})
			if err != nil {
				t.Fatalf("offset %d: recovery failed: %v", offset, err)
			}
			if w2.CurrentLSN() != int64(wantLSN) {
				t.Fatalf("offset %d: recovered watermark %d, want %d", offset, w2.CurrentLSN(), wantLSN)
			}
			if len(got) != want {
				t.Fatalf("offset %d: recovered %d records, want %d (batch atomicity broken)", offset, len(got), want)
			}
			for i := range got {
				if got[i].lsn != 0 && got[i].payload == nil {
					t.Fatalf("offset %d: record %d empty payload", offset, i)
				}
			}
			if err := w2.Close(); err != nil {
				t.Fatalf("offset %d: close: %v", offset, err)
			}

			// The engine must agree with the model byte-for-byte: recover
			// through NewLedger (which applies to real accounts) and compare
			// every balance.
			w3, err := wal.Open(caseDir, 1<<20)
			if err != nil {
				t.Fatalf("offset %d: reopen: %v", offset, err)
			}
			l, err := engine.NewLedger(engine.DefaultConfig(), w3)
			if err != nil {
				t.Fatalf("offset %d: ledger: %v", offset, err)
			}
			for id := range model.credits {
				acc, err := l.GetAccount(context.Background(), id)
				if err != nil {
					t.Fatalf("offset %d: account %x missing after recovery: %v", offset, id, err)
				}
				bal, _ := core.Balance(acc)
				wantBal := model.credits[id] - model.debits[id]
				if bal.Lo != wantBal {
					t.Fatalf("offset %d: balance for %x = %d, model says %d", offset, id, bal.Lo, wantBal)
				}
			}
			if err := l.Close(); err != nil {
				t.Fatalf("offset %d: close ledger: %v", offset, err)
			}
		}()
	}
}
