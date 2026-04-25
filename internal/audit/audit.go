// Package audit is an independent replayer for aequitas WAL directories: it
// grades the ledger with code that shares none of the engine's assumptions
// (T3.4). It re-implements the frame format, batch atomicity rules, LSN
// ordering, and the conservation invariant from scratch — the only code it
// reuses is the payload codecs, because the wire format is the shared
// contract by definition.
//
// Unlike engine recovery, the auditor does NOT heal anything: a corrupt
// frame, a count-mismatched batch, or a torn tail is reported as a violation
// rather than truncated away, so operators see exactly what a crash left
// behind.
package audit

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"aequitas-ledger/internal/core"
)

const (
	headerSize = 13 // [LSN:8][type:1][len:4]
	crcSize    = 4
)

// ViolationKind categorizes what the auditor found.
type ViolationKind string

const (
	ViolationCorruptFrame  ViolationKind = "corrupt_frame"
	ViolationTornTail      ViolationKind = "torn_tail"
	ViolationBatchCount    ViolationKind = "batch_count_mismatch"
	ViolationLSNOrder      ViolationKind = "lsn_order"
	ViolationConservation  ViolationKind = "conservation"
	ViolationDuplicateKey  ViolationKind = "duplicate_idempotency_key"
	ViolationUnknownRecord ViolationKind = "unknown_record_type"
)

// Violation is one finding.
type Violation struct {
	Kind    ViolationKind
	Segment string
	LSN     uint64
	Detail  string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s [%s lsn=%d]: %s", v.Kind, v.Segment, v.LSN, v.Detail)
}

// Report summarizes a full audit pass.
type Report struct {
	Segments   int
	Records    int
	Batches    int
	Accounts   int
	Transfers  int
	TornTail   *Violation // set when the WAL ends in an uncommitted batch
	Violations []Violation
}

// Valid reports whether the WAL passed every check.
func (r Report) Valid() bool { return len(r.Violations) == 0 }

// Audit walks every segment in dir (ordered by id) and verifies:
//
//  1. every frame's CRC32 is intact,
//  2. every batch's commit marker matches its accumulated record count,
//  3. LSNs are strictly increasing across the whole WAL,
//  4. replaying the records conserves funds: every account's balance is
//     non-negative and the system-wide balance equals the credits injected
//     at account creation,
//  5. no idempotency key is used by two different transfer IDs, and no
//     transfer ID appears twice.
func Audit(dir string) (Report, error) {
	rep := Report{}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return rep, fmt.Errorf("read wal dir: %w", err)
	}
	var ids []int
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(name, "wal-%d.seg", &id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	rep.Segments = len(ids)

	type bal struct{ credits, debits uint64 }
	balances := map[[16]byte]*bal{}
	injected := uint64(0)
	keys := map[[32]byte][16]byte{} // idempotency key → transfer ID
	transferIDs := map[[16]byte]string{}

	pendingCount := 0
	prevLSN := uint64(0)
	violate := func(kind ViolationKind, seg string, lsn uint64, format string, args ...any) {
		rep.Violations = append(rep.Violations, Violation{
			Kind: kind, Segment: seg, LSN: lsn, Detail: fmt.Sprintf(format, args...),
		})
	}

	for _, id := range ids {
		name := fmt.Sprintf("wal-%06d.seg", id)
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return rep, fmt.Errorf("read segment %s: %w", name, err)
		}
		off := 0
		for off+headerSize <= len(data) {
			lsn := binary.BigEndian.Uint64(data[off : off+8])
			typ := data[off+8]
			plen := int(binary.BigEndian.Uint32(data[off+9 : off+13]))
			frameLen := headerSize + plen + crcSize
			if plen < 0 || off+frameLen > len(data) {
				v := Violation{
					Kind: ViolationTornTail, Segment: name, LSN: lsn,
					Detail: fmt.Sprintf("torn frame at byte %d extends past the %d-byte segment", off, len(data)),
				}
				rep.Violations = append(rep.Violations, v)
				rep.TornTail = &v
				break
			}
			frame := data[off : off+frameLen]
			if crc32.ChecksumIEEE(frame[:headerSize+plen]) != binary.BigEndian.Uint32(frame[headerSize+plen:]) {
				violate(ViolationCorruptFrame, name, lsn, "CRC mismatch at byte %d", off)
				off += frameLen
				rep.Records++
				continue
			}
			payload := frame[headerSize : headerSize+plen]
			off += frameLen
			rep.Records++

			if prevLSN > 0 && lsn <= prevLSN {
				violate(ViolationLSNOrder, name, lsn, "LSN %d not greater than previous %d", lsn, prevLSN)
			}
			prevLSN = lsn

			switch typ {
			case 1: // transfer
				rep.Transfers++
				tr, derr := core.DecodeTransferPayload(payload)
				if derr != nil {
					violate(ViolationCorruptFrame, name, lsn, "transfer payload does not decode: %v", derr)
					continue
				}
				if b := balances[tr.DebitAccountID]; b != nil {
					b.debits += tr.Amount.Lo
				} else {
					violate(ViolationConservation, name, lsn, "transfer debits unknown account %x", tr.DebitAccountID)
				}
				if b := balances[tr.CreditAccountID]; b != nil {
					b.credits += tr.Amount.Lo
				} else {
					violate(ViolationConservation, name, lsn, "transfer credits unknown account %x", tr.CreditAccountID)
				}
				if tr.IdempotencyKey != ([32]byte{}) {
					if prev, ok := keys[tr.IdempotencyKey]; ok && prev != tr.ID {
						violate(ViolationDuplicateKey, name, lsn, "key used by transfers %x and %x", prev, tr.ID)
					} else if !ok {
						keys[tr.IdempotencyKey] = tr.ID
					}
				}
				if prevSeg, dup := transferIDs[tr.ID]; dup {
					violate(ViolationDuplicateKey, name, lsn, "transfer ID %x already seen in %s", tr.ID, prevSeg)
				} else {
					transferIDs[tr.ID] = name
				}
				pendingCount++
			case 2: // account
				rep.Accounts++
				acc, derr := core.DecodeAccountPayload(payload)
				if derr != nil {
					violate(ViolationCorruptFrame, name, lsn, "account payload does not decode: %v", derr)
					continue
				}
				if _, exists := balances[acc.ID]; exists {
					// Duplicate account record (follower re-sync): state is
					// already tracked; nothing to inject.
					pendingCount++
					continue
				}
				balances[acc.ID] = &bal{credits: acc.PostedCredits.Lo, debits: acc.PostedDebits.Lo}
				if acc.PostedDebits.Lo > acc.PostedCredits.Lo {
					violate(ViolationConservation, name, lsn,
						"account %x created with debits %d > credits %d", acc.ID, acc.PostedDebits.Lo, acc.PostedCredits.Lo)
				}
				injected += acc.PostedCredits.Lo - acc.PostedDebits.Lo
				pendingCount++
			case 4: // batch commit
				rep.Batches++
				var expected uint32
				if plen >= 12 {
					expected = binary.BigEndian.Uint32(payload[8:12])
				}
				if uint32(pendingCount) != expected {
					violate(ViolationBatchCount, name, lsn,
						"commit expects %d records, %d accumulated since the last commit", expected, pendingCount)
				}
				pendingCount = 0
			case 3: // checkpoint: metadata only
				pendingCount++
			default:
				violate(ViolationUnknownRecord, name, lsn, "record type %d", typ)
			}
		}
	}

	// Final conservation: every balance non-negative; the system-wide balance
	// equals what account creation injected (transfers only move funds).
	var total uint64
	for id, b := range balances {
		if b.debits > b.credits {
			violate(ViolationConservation, "", 0,
				"account %x ends with debits %d > credits %d", id, b.debits, b.credits)
			continue
		}
		total += b.credits - b.debits
	}
	if len(balances) > 0 && total != injected {
		violate(ViolationConservation, "", 0,
			"system balance %d != credits injected at account creation %d", total, injected)
	}

	return rep, nil
}
