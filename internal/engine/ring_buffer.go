package engine

import (
	"runtime"
	"sync/atomic"

	"aequitas-ledger/internal/core"
)

type RingBuffer struct {
	buf   []TransferEvent
	ready []atomic.Uint64
	mask  uint64
	cap   uint64

	head atomic.Uint64 // producer claim sequence
	tail uint64        // consumer-owned sequence

	tailShared atomic.Uint64 // producer-visible tail snapshot
}

func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 || (size&(size-1)) != 0 {
		panic("ring buffer size must be power of 2 and > 0")
	}
	rb := &RingBuffer{
		buf:   make([]TransferEvent, size),
		ready: make([]atomic.Uint64, size),
		mask:  uint64(size - 1),
		cap:   uint64(size),
	}
	rb.tailShared.Store(0)
	return rb
}

func (rb *RingBuffer) Submit(ev TransferEvent) error {
	for {
		h := rb.head.Load()
		t := rb.tailShared.Load()
		if h-t >= rb.cap {
			return core.ErrBufferFull{}
		}
		if rb.head.CompareAndSwap(h, h+1) {
			idx := h & rb.mask
			rb.buf[idx] = ev
			rb.ready[idx].Store(h + 1)
			return nil
		}
		runtime.Gosched()
	}
}

func (rb *RingBuffer) DrainBatch(max int) []TransferEvent {
	if max <= 0 {
		return nil
	}
	head := rb.head.Load()
	if head == rb.tail {
		return nil
	}

	// Size the batch to what is actually pending, not max: with a 10k
	// maxBatchSize a batch-of-one would otherwise preallocate ~1.2 MB.
	pending := head - rb.tail
	if uint64(max) > pending {
		max = int(pending)
	}
	out := make([]TransferEvent, 0, max)
	for len(out) < max && rb.tail < head {
		idx := rb.tail & rb.mask
		wantReady := rb.tail + 1
		if rb.ready[idx].Load() != wantReady {
			break
		}
		out = append(out, rb.buf[idx])
		rb.tail++
	}
	rb.tailShared.Store(rb.tail)
	if len(out) == 0 {
		return nil
	}
	return out
}

func (rb *RingBuffer) Len() int {
	head := rb.head.Load()
	tail := rb.tailShared.Load()
	if head < tail {
		return 0
	}
	return int(head - tail)
}
