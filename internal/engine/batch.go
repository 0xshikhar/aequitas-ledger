package engine

import (
	"runtime"
	"time"
)

type Batcher struct {
	rb           *RingBuffer
	maxBatchSize int
	timeout      time.Duration
}

func NewBatcher(rb *RingBuffer, maxSize int, timeout time.Duration) *Batcher {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	if timeout < 0 {
		timeout = 0
	}
	return &Batcher{rb: rb, maxBatchSize: maxSize, timeout: timeout}
}

// Collect drains up to maxBatchSize events, waiting no longer than timeout for
// the batch to fill. yield, when non-nil, is consulted while waiting: if it
// reports pending control work (reads, account creates, snapshot requests),
// Collect returns immediately — nil when the ring buffer is empty, a partial
// batch otherwise — so the event loop can service it instead of spinning for a
// fuller batch (C0.16).
func (b *Batcher) Collect(yield func() bool) []TransferEvent {
	if b.rb == nil {
		return nil
	}
	deadline := time.Now().Add(b.timeout)
	for b.rb.Len() == 0 {
		if b.timeout == 0 || time.Now().After(deadline) {
			return nil
		}
		if yield != nil && yield() {
			return nil
		}
		runtime.Gosched()
	}

	deadline = time.Now().Add(b.timeout)
	for b.rb.Len() < b.maxBatchSize {
		if b.timeout == 0 || time.Now().After(deadline) {
			break
		}
		if yield != nil && yield() {
			break
		}
		runtime.Gosched()
	}

	return b.rb.DrainBatch(b.maxBatchSize)
}
