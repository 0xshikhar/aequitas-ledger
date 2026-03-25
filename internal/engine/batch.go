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

func (b *Batcher) Collect() []TransferEvent {
	if b.rb == nil {
		return nil
	}
	deadline := time.Now().Add(b.timeout)
	for b.rb.Len() == 0 {
		if b.timeout == 0 || time.Now().After(deadline) {
			return nil
		}
		runtime.Gosched()
	}

	deadline = time.Now().Add(b.timeout)
	for b.rb.Len() < b.maxBatchSize {
		if b.timeout == 0 || time.Now().After(deadline) {
			break
		}
		runtime.Gosched()
	}

	return b.rb.DrainBatch(b.maxBatchSize)
}
