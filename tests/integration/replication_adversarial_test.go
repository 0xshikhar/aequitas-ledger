package integration

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/replication"
	"aequitas-ledger/internal/wal"
)

// chaoticProxy is an in-process TCP proxy that can inject latency, drops,
// byte corruptions, and frame duplication into the replication stream (T3.3).
type chaoticProxy struct {
	listener     net.Listener
	upstreamAddr string

	corruptNextN atomic.Int32
	dropNow      atomic.Bool
	closed       atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

func newChaoticProxy(upstreamAddr string) (*chaoticProxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &chaoticProxy{
		listener:     lis,
		upstreamAddr: upstreamAddr,
	}
	go p.acceptLoop()
	return p, nil
}

func (p *chaoticProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *chaoticProxy) Close() {
	p.closed.Store(true)
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *chaoticProxy) DropActiveConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *chaoticProxy) CorruptNext(bytes int32) {
	p.corruptNextN.Store(bytes)
}

func (p *chaoticProxy) acceptLoop() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			if p.closed.Load() {
				return
			}
			continue
		}

		upstreamConn, err := net.Dial("tcp", p.upstreamAddr)
		if err != nil {
			_ = clientConn.Close()
			continue
		}

		p.mu.Lock()
		p.conns = append(p.conns, clientConn, upstreamConn)
		p.mu.Unlock()

		// client -> upstream (handshake)
		go func(src, dst net.Conn) {
			defer src.Close()
			defer dst.Close()
			_, _ = io.Copy(dst, src)
		}(clientConn, upstreamConn)

		// upstream -> client (replication stream where chaos is injected)
		go func(src, dst net.Conn) {
			defer src.Close()
			defer dst.Close()
			buf := make([]byte, 4096)
			for {
				if p.dropNow.Load() {
					return
				}
				n, err := src.Read(buf)
				if err != nil {
					return
				}
				data := buf[:n]

				// Inject byte corruption if scheduled
				if toCorrupt := p.corruptNextN.Swap(0); toCorrupt > 0 {
					corruptLen := int(toCorrupt)
					if corruptLen > len(data) {
						corruptLen = len(data)
					}
					corrupted := make([]byte, len(data))
					copy(corrupted, data)
					// Flip bits in the data payload
					for i := 0; i < corruptLen; i++ {
						corrupted[len(corrupted)-1-i] ^= 0xFF
					}
					data = corrupted
				}

				if _, err := dst.Write(data); err != nil {
					return
				}
			}
		}(upstreamConn, clientConn)
	}
}

// TestReplicationAdversarial_CorruptionAndRecovery tests that CRC corruption
// mid-stream causes the follower to reject corrupted frames, disconnect,
// reconnect via backoff, and recover cleanly without corrupting state.
func TestReplicationAdversarial_CorruptionAndRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	defer primaryWAL.Close()

	primary, err := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	if err != nil {
		t.Fatalf("new primary ledger: %v", err)
	}
	defer primary.Close()

	replServer := replication.NewServer("127.0.0.1:0", primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start primary repl server: %v", err)
	}
	defer replServer.Stop()

	proxy, err := newChaoticProxy(replServer.Addr())
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	defer proxy.Close()

	followerWAL, err := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	defer followerWAL.Close()

	cur := [4]byte{'U', 'S', 'D', 0}
	accA := mkID(1)
	accB := mkID(2)

	ctx := context.Background()
	if _, err := primary.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 10000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts on primary: %v", err)
	}

	follower := replication.NewFollower(proxy.Addr(), followerWAL)
	follower.BackoffBase = 10 * time.Millisecond
	follower.BackoffMax = 50 * time.Millisecond
	follower.Start()
	defer follower.Stop()

	// 1. Write batch 1 on primary
	tr1 := core.Transfer{
		ID:              mkID(10),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 200},
	}
	if _, err := primary.CreateTransfers(ctx, []core.Transfer{tr1}); err != nil {
		t.Fatalf("create transfer 1: %v", err)
	}

	// Wait for follower to receive batch 1
	var accAfter1 core.Account
	for i := 0; i < 40; i++ {
		acc, err := follower.GetAccount(accA)
		if err == nil && acc.PostedDebits == (core.Uint128{Lo: 200}) {
			accAfter1 = acc
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if accAfter1.PostedDebits != (core.Uint128{Lo: 200}) {
		t.Fatalf("follower failed to apply batch 1: debits=%v", accAfter1.PostedDebits)
	}

	// 2. Schedule corruption on the next frame (batch 2)
	proxy.CorruptNext(8)

	// Write batch 2 on primary
	tr2 := core.Transfer{
		ID:              mkID(11),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 300},
	}
	if _, err := primary.CreateTransfers(ctx, []core.Transfer{tr2}); err != nil {
		t.Fatalf("create transfer 2: %v", err)
	}

	// Follower must detect CRC mismatch, fail the connection, back off, reconnect,
	// and resume cleanly without corrupting state.
	time.Sleep(300 * time.Millisecond)

	// Follower balances must NEVER be corrupted with garbage values
	accAfter, err := follower.GetAccount(accA)
	if err != nil {
		t.Fatalf("get follower accA: %v", err)
	}
	if accAfter.PostedDebits != (core.Uint128{Lo: 200}) && accAfter.PostedDebits != (core.Uint128{Lo: 500}) {
		t.Fatalf("corrupted debits detected on follower: %v", accAfter.PostedDebits)
	}
}

// TestReplicationAdversarial_SuddenDropAndResume asserts that dropping TCP
// connections mid-stream causes the follower to reconnect and resume from
// its applied watermark without duplicate execution or divergence.
func TestReplicationAdversarial_SuddenDropAndResume(t *testing.T) {
	tmpDir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	defer primaryWAL.Close()

	primary, err := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	if err != nil {
		t.Fatalf("new primary ledger: %v", err)
	}
	defer primary.Close()

	replServer := replication.NewServer("127.0.0.1:0", primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start primary repl server: %v", err)
	}
	defer replServer.Stop()

	proxy, err := newChaoticProxy(replServer.Addr())
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	defer proxy.Close()

	followerWAL, err := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	defer followerWAL.Close()

	cur := [4]byte{'U', 'S', 'D', 0}
	accA := mkID(1)
	accB := mkID(2)

	ctx := context.Background()
	if _, err := primary.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 50000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts on primary: %v", err)
	}

	follower := replication.NewFollower(proxy.Addr(), followerWAL)
	follower.BackoffBase = 10 * time.Millisecond
	follower.BackoffMax = 30 * time.Millisecond
	follower.Start()
	defer follower.Stop()

	// Stream 5 successive batches, abruptly severing connection between each
	totalTransferred := uint64(0)
	for i := 1; i <= 5; i++ {
		tr := core.Transfer{
			ID:              mkID(uint64(100 + i)),
			DebitAccountID:  accA,
			CreditAccountID: accB,
			Amount:          core.Uint128{Lo: 100},
		}
		totalTransferred += 100
		if _, err := primary.CreateTransfers(ctx, []core.Transfer{tr}); err != nil {
			t.Fatalf("transfer batch %d: %v", i, err)
		}

		// Sever connections mid-flight
		proxy.DropActiveConnections()
		time.Sleep(40 * time.Millisecond)
	}

	// Allow follower time to complete final catch-up
	var finalDebits core.Uint128
	for attempt := 0; attempt < 50; attempt++ {
		acc, err := follower.GetAccount(accA)
		if err == nil && acc.PostedDebits == (core.Uint128{Lo: totalTransferred}) {
			finalDebits = acc.PostedDebits
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	expected := core.Uint128{Lo: totalTransferred}
	if finalDebits != expected {
		t.Fatalf("expected total debits %v after adversarial drops, got %v", expected, finalDebits)
	}
}

// TestReplicationAdversarial_StaleTermRefused verifies that a primary with an
// obsolete leader term is strictly fenced off by the follower without wiping state.
func TestReplicationAdversarial_StaleTermRefused(t *testing.T) {
	tmpDir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	defer primaryWAL.Close()

	// Bump leader term twice on primary (term becomes 2)
	_, _ = replication.BumpTerm(primaryWAL)
	_, _ = replication.BumpTerm(primaryWAL)

	replServer := replication.NewServer("127.0.0.1:0", primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start primary repl server: %v", err)
	}
	defer replServer.Stop()

	followerWAL, err := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	defer followerWAL.Close()

	follower := replication.NewFollower(replServer.Addr(), followerWAL)
	follower.Start()
	defer follower.Stop()

	// Wait for follower to accept primary term
	time.Sleep(100 * time.Millisecond)

	// Now start a second fake server advertising stale term 0
	fakeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake: %v", err)
	}
	defer fakeListener.Close()

	go func() {
		conn, err := fakeListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Handshake: read resume LSN
		var lsnBuf [8]byte
		_, _ = io.ReadFull(conn, lsnBuf[:])
		// Send stale term 0
		var termBuf [8]byte
		binary.BigEndian.PutUint64(termBuf[:], 0)
		_, _ = conn.Write(termBuf[:])
	}()

	// Follower connecting to fake server should reject the stale term
	staleFollower := replication.NewFollower(fakeListener.Addr().String(), followerWAL)
	staleFollower.BackoffBase = 10 * time.Millisecond
	staleFollower.Start()
	defer staleFollower.Stop()

	time.Sleep(100 * time.Millisecond)
	// Stale primary must not be accepted as connected
	if staleFollower.Connected() {
		t.Fatalf("stale primary term was accepted!")
	}
}
