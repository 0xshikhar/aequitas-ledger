package engine

import (
	"time"
)

type Batcher struct {
	rb           *RingBuffer
	maxBatchSize int
	timeout      time.Duration

	// Single-consumer reuse (the event loop is the only caller): avoids a
	// timer/ticker allocation per Collect.
	timer *time.Timer
	tick  *time.Ticker
}

// yieldPollInterval bounds how long control work (reads, account creates,
// snapshot views) can wait while the batcher holds the loop in a wait state.
const yieldPollInterval = 100 * time.Microsecond

func NewBatcher(rb *RingBuffer, maxSize int, timeout time.Duration) *Batcher {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	if timeout < 0 {
		timeout = 0
	}
	return &Batcher{rb: rb, maxBatchSize: maxSize, timeout: timeout}
}

// Depth reports an approximation of the pending event count (the ring
// buffer's producer/consumer snapshot may skew by a few events mid-drain).
// Used for the ringbuffer_depth gauge — scheduling hints, not accounting.
func (b *Batcher) Depth() int {
	if b.rb == nil {
		return 0
	}
	return b.rb.Len()
}

// Idle reports whether the ring buffer is empty. The event loop uses it to
// enter a blocking wait on real wakeup sources instead of spinning.
func (b *Batcher) Idle() bool {
	return b.rb == nil || b.rb.Len() == 0
}

// ArriveChan exposes the ring buffer's arrival signal for the event loop's
// idle wait.
func (b *Batcher) ArriveChan() <-chan struct{} {
	return b.rb.arriveChan()
}

// waitTimer returns the batcher's reused timer armed for d (safe to call
// again while armed: any pending fire is drained first).
func (b *Batcher) waitTimer(d time.Duration) *time.Timer {
	if b.timer == nil {
		b.timer = time.NewTimer(d)
		return b.timer
	}
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
	b.timer.Reset(d)
	return b.timer
}

func (b *Batcher) yieldTicker() *time.Ticker {
	if b.tick == nil {
		b.tick = time.NewTicker(yieldPollInterval)
	}
	return b.tick
}

// Collect drains up to maxBatchSize events, waiting no longer than timeout
// for the batch to fill. It NEVER spins (S2.1): when the ring is empty the
// goroutine blocks on the ring's arrival channel, and the fill phase blocks
// on the arrival channel or the deadline instead of polling the CPU.
// yield, when non-nil, is polled on a short ticker (and checked between
// waits): if it reports pending control work (reads, account creates,
// snapshot requests), Collect returns immediately so the event loop can
// service it instead of waiting for a fuller batch (C0.16).
func (b *Batcher) Collect(yield func() bool) []TransferEvent {
	if b.rb == nil {
		return nil
	}

	// Idle: block until a producer arrives, the timeout elapses, or control
	// work pends.
	if b.rb.Len() == 0 {
		t := b.waitTimer(b.timeout)
		tick := b.yieldTicker()
		for b.rb.Len() == 0 {
			ch := b.rb.arriveChan()
			if b.rb.Len() != 0 {
				break // a producer landed between the check and registration
			}
			select {
			case <-ch:
			case <-t.C:
				return nil
			case <-tick.C:
				if yield != nil && yield() {
					return nil
				}
			}
		}
	}

	// Fill: wait for the batch to reach maxBatchSize or the deadline.
	deadline := time.Now().Add(b.timeout)
	for b.rb.Len() < b.maxBatchSize {
		if yield != nil && yield() {
			break
		}
		remain := deadline.Sub(time.Now())
		if remain <= 0 {
			break
		}
		t := b.waitTimer(remain)
		tick := b.yieldTicker()
		ch := b.rb.arriveChan()
		select {
		case <-ch:
		case <-t.C:
		case <-tick.C:
		}
	}

	return b.rb.DrainBatch(b.maxBatchSize)
}
