package replication

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"aequitas-ledger/internal/wal"
)

// Leader-term fencing (C0.8.3): every primary carries a monotonically
// increasing term. A primary bumps its term exactly once per promotion; a
// follower persists the highest term it has accepted and refuses streams
// from primaries with a lower term — a stale primary resurrecting after a
// failover can no longer feed a follower divergent history.
//
// Terms live in small files beside the WAL: the primary's own term in
// replication.term, the follower's last-accepted term in
// replication.follower-term. In-memory values are authoritative while
// running; the files only survive restarts.

const (
	primaryTermFile  = "replication.term"
	followerTermFile = "replication.follower-term"
)

func loadTermUint(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil || len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func storeTermUint(path string, term uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], term)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b[:], 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path) // atomic write-then-rename
}

// PrimaryTerm loads the primary's term, initializing it to 1 on first start
// (a fresh primary is term 1; only promotion ever increments it).
func PrimaryTerm(w *wal.WAL) (uint64, error) {
	path := filepath.Join(w.Dir(), primaryTermFile)
	term := loadTermUint(path)
	if term > 0 {
		return term, nil
	}
	term = 1
	if err := storeTermUint(path, term); err != nil {
		return 0, fmt.Errorf("initialize replication term: %w", err)
	}
	return term, nil
}

// BumpTerm increments and persists the primary's term. Called exactly once
// per follower promotion so the promoted primary outranks the old one.
func BumpTerm(w *wal.WAL) (uint64, error) {
	path := filepath.Join(w.Dir(), primaryTermFile)
	term := loadTermUint(path) + 1
	if err := storeTermUint(path, term); err != nil {
		return 0, fmt.Errorf("bump replication term: %w", err)
	}
	return term, nil
}

func followerTermPath(w *wal.WAL) string {
	return filepath.Join(w.Dir(), followerTermFile)
}

func loadFollowerTerm(w *wal.WAL) uint64 {
	return loadTermUint(followerTermPath(w))
}

func storeFollowerTerm(w *wal.WAL, term uint64) error {
	return storeTermUint(followerTermPath(w), term)
}
