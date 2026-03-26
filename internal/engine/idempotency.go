package engine

import (
	"sync"
	"time"

	"aequitas-ledger/internal/core"
)

type idEntry struct {
	key       [32]byte
	transfer  core.Transfer
	pending   bool
	expiresAt time.Time
	prev      *idEntry
	next      *idEntry
}

type IdempotencyStore struct {
	maxSize int
	ttl     time.Duration

	mu      sync.RWMutex
	entries map[[32]byte]*idEntry
	head    *idEntry
	tail    *idEntry
	size    int
}

func NewIdempotencyStore(maxSize int, ttl time.Duration) *IdempotencyStore {
	if maxSize <= 0 {
		maxSize = 100_000
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &IdempotencyStore{
		maxSize: maxSize,
		ttl:     ttl,
		entries: make(map[[32]byte]*idEntry, maxSize),
	}
}

func (s *IdempotencyStore) CheckAndReserve(key [32]byte) (core.Transfer, bool, error) {
	now := time.Now()

	s.mu.RLock()
	if e, ok := s.entries[key]; ok {
		if e.pending {
			s.mu.RUnlock()
			return core.Transfer{}, false, core.ErrIdempotencyConflict{Key: key}
		}
		if now.Before(e.expiresAt) {
			res := e.transfer
			s.mu.RUnlock()
			return res, true, nil
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked(now)

	if e, ok := s.entries[key]; ok {
		if e.pending {
			return core.Transfer{}, false, core.ErrIdempotencyConflict{Key: key}
		}
		if now.Before(e.expiresAt) {
			s.touchLocked(e)
			return e.transfer, true, nil
		}
		s.removeLocked(e)
	}

	e := &idEntry{key: key, pending: true, expiresAt: now.Add(s.ttl)}
	s.entries[key] = e
	s.insertHeadLocked(e)
	s.size++
	return core.Transfer{}, false, nil
}

func (s *IdempotencyStore) Commit(key [32]byte, result core.Transfer) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked(now)

	e, ok := s.entries[key]
	if !ok {
		e = &idEntry{key: key}
		s.entries[key] = e
		s.insertHeadLocked(e)
		s.size++
	}
	e.pending = false
	e.transfer = result
	e.expiresAt = now.Add(s.ttl)
	s.touchLocked(e)

	for s.size > s.maxSize {
		s.evictLRULocked()
	}
}

func (s *IdempotencyStore) Rollback(key [32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && e.pending {
		s.removeLocked(e)
	}
}

func (s *IdempotencyStore) evictExpiredLocked(now time.Time) {
	for e := s.tail; e != nil; {
		prev := e.prev
		if now.Before(e.expiresAt) {
			break
		}
		s.removeLocked(e)
		e = prev
	}
}

func (s *IdempotencyStore) evictLRULocked() {
	for e := s.tail; e != nil; e = e.prev {
		if e.pending {
			continue
		}
		s.removeLocked(e)
		return
	}
}

func (s *IdempotencyStore) touchLocked(e *idEntry) {
	if s.head == e {
		return
	}
	s.unlinkLocked(e)
	s.insertHeadLocked(e)
}

func (s *IdempotencyStore) insertHeadLocked(e *idEntry) {
	e.prev = nil
	e.next = s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

func (s *IdempotencyStore) unlinkLocked(e *idEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (s *IdempotencyStore) removeLocked(e *idEntry) {
	s.unlinkLocked(e)
	delete(s.entries, e.key)
	s.size--
}
