package unit

import (
	"sync"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
)

func TestRingBufferConcurrentSubmitNoLossNoDup(t *testing.T) {
	rb, _ := engine.NewRingBuffer(1 << 15)
	const producers = 100
	const perProducer = 200
	const total = producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := uint64(p*perProducer + i)
				ev := engine.NewTransferEvent(core.Transfer{ID: id16(id), Amount: core.FromUint64(1)})
				for {
					if err := rb.Submit(ev); err == nil {
						break
					}
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	seen := make(map[[16]byte]struct{}, total)
	recv := 0
	for recv < total {
		batch := rb.DrainBatch(1024)
		if len(batch) == 0 {
			time.Sleep(time.Microsecond)
			continue
		}
		for _, ev := range batch {
			if _, dup := seen[ev.Transfer.ID]; dup {
				t.Fatalf("duplicate event id: %x", ev.Transfer.ID)
			}
			seen[ev.Transfer.ID] = struct{}{}
			recv++
		}
	}
	wg.Wait()

	if recv != total {
		t.Fatalf("lost events: got=%d want=%d", recv, total)
	}
}

// S2.1: a consumer blocked on the arrival channel must wake when a producer
// publishes — this is what replaced the event loop's idle busy-spin.
func TestRingBufferArrivalWakeup(t *testing.T) {
	rb, _ := engine.NewRingBuffer(1 << 10)

	woken := make(chan struct{})
	go func() {
		ch := rb.ArriveChanForTest()
		<-ch
		close(woken)
	}()

	time.Sleep(50 * time.Millisecond) // let the consumer register
	if err := rb.Submit(engine.NewTransferEvent(core.Transfer{ID: id16(1), Amount: core.FromUint64(1)})); err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case <-woken:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer was not woken by the producer's arrival signal")
	}
}
