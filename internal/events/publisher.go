package events

import (
	"sync"

	"aequitas-ledger/internal/core"
)

type EventType string

const (
	EventTypeTransferProcessed EventType = "transfer.processed"
	EventTypeAccountCreated     EventType = "account.created"
)

type Event struct {
	Type      EventType     `json:"type"`
	LSN       int64         `json:"lsn"`
	Timestamp int64         `json:"timestamp"`
	Transfer  core.Transfer `json:"transfer,omitempty"`
	Account   core.Account  `json:"account,omitempty"`
}

type Subscriber func(ev Event)

type Publisher struct {
	mu          sync.RWMutex
	subscribers []Subscriber
}

func NewPublisher() *Publisher {
	return &Publisher{
		subscribers: make([]Subscriber, 0),
	}
}

func (p *Publisher) Subscribe(sub Subscriber) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, sub)
}

func (p *Publisher) Publish(ev Event) {
	p.mu.RLock()
	subs := make([]Subscriber, len(p.subscribers))
	copy(subs, p.subscribers)
	p.mu.RUnlock()

	for _, sub := range subs {
		sub(ev)
	}
}
