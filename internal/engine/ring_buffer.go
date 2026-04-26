package engine

import (
	"runtime"
	"sync"
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

	// Arrival signaling (S2.1): lets the consumer block — instead of
	// spinning — until a producer publishes. waitMu guards the channel
	// swap; waiting is an atomic fast-path flag so producers pay one load
	// when nobody waits.
	notifyMu sync.Mutex
	arrive   chan struct{}
	waiting  atomic.Bool
}

// NewRingBuffer creates a ring buffer. size must be a power of two and > 0 —
// the mask-based indexing depends on it — and invalid sizes return an error
// rather than panicking, so callers can fail fast with a clean message.
func NewRingBuffer(size int) (*RingBuffer, error) {
	if size <= 0 || (size&(size-1)) != 0 {
		return nil, core.ErrInvalidRingBufferSize{Size: size}
	}
	rb := &RingBuffer{
		buf:   make([]TransferEvent, size),
		ready: make([]atomic.Uint64, size),
		mask:  uint64(size - 1),
		cap:   uint64(size),
	}
	rb.tailShared.Store(0)
	return rb, nil
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
			rb.signalArrived()
			return nil
		}
		runtime.Gosched()
	}
}

// arriveChan returns the channel the consumer can block on for the next
// arrival. It also arms the waiting flag, so any Submit that publishes after
// this call is guaranteed to signal.
func (rb *RingBuffer) arriveChan() <-chan struct{} {
	rb.notifyMu.Lock()
	defer rb.notifyMu.Unlock()
	if rb.head.Load() > rb.tailShared.Load() {
		if rb.arrive != nil {
			close(rb.arrive)
			rb.arrive = nil
		}
		rb.waiting.Store(false)
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh
	}
	if rb.arrive == nil {
		rb.arrive = make(chan struct{})
	}
	rb.waiting.Store(true)
	return rb.arrive
}

// ArriveChanForTest exposes the arrival channel to external tests.
func (rb *RingBuffer) ArriveChanForTest() <-chan struct{} { return rb.arriveChan() }

// signalArrived wakes a blocked consumer, if one is waiting.
func (rb *RingBuffer) signalArrived() {
	rb.notifyMu.Lock()
	if rb.arrive != nil {
		close(rb.arrive)
		rb.arrive = nil
	}
	rb.waiting.Store(false)
	rb.notifyMu.Unlock()
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
