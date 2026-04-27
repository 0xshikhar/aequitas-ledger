package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/replication"
	"aequitas-ledger/internal/wal"
)

func TestReplicationTLS_EndToEndAndPlaintextRejection(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Generate self-signed TLS certificates
	certPEM, keyPEM, err := replication.GenerateSelfSignedCert("localhost", "127.0.0.1")
	if err != nil {
		t.Fatalf("generate test cert: %v", err)
	}

	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	serverTLS, err := replication.NewServerTLSConfig(certFile, keyFile, "", false)
	if err != nil {
		t.Fatalf("new server tls: %v", err)
	}

	// 2. Start primary with TLS replication listener
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
	replServer.SetTLSConfig(serverTLS)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start primary repl server: %v", err)
	}
	defer replServer.Stop()

	// 3. Verify that an unencrypted plaintext client fails to establish a replication stream
	plainConn, err := net.Dial("tcp", replServer.Addr())
	if err == nil {
		// Send handshake over plaintext
		_, _ = plainConn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 1})
		var buf [8]byte
		_ = plainConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, readErr := plainConn.Read(buf[:])
		// Plaintext client MUST NOT receive valid term, read should fail or return EOF
		if readErr == nil {
			t.Fatalf("plaintext client unexpectedly read valid response from TLS listener")
		}
		_ = plainConn.Close()
	}

	// 4. Start follower configured with TLS root CA
	followerWAL, err := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	defer followerWAL.Close()

	clientTLS, err := replication.NewClientTLSConfig("", "", certFile, false)
	if err != nil {
		t.Fatalf("new client tls: %v", err)
	}

	follower := replication.NewFollower(replServer.Addr(), followerWAL)
	follower.SetTLSConfig(clientTLS)
	follower.Start()
	defer follower.Stop()

	// 5. Submit transactions on primary and assert follower replicates cleanly over TLS
	ctx := context.Background()
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'E', 'U', 'R', 0}

	if _, err := primary.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 50000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts on primary: %v", err)
	}

	tr := core.Transfer{
		ID:              mkID(10),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 1250},
	}
	if _, err := primary.CreateTransfers(ctx, []core.Transfer{tr}); err != nil {
		t.Fatalf("create transfer on primary: %v", err)
	}

	// Wait for follower to replicate over encrypted TLS
	var accAfter core.Account
	for i := 0; i < 40; i++ {
		acc, err := follower.GetAccount(accA)
		if err == nil && acc.PostedDebits == (core.Uint128{Lo: 1250}) {
			accAfter = acc
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if accAfter.PostedDebits != (core.Uint128{Lo: 1250}) {
		t.Fatalf("follower failed to replicate over TLS: debits=%v", accAfter.PostedDebits)
	}
	if !follower.Connected() {
		t.Fatalf("expected follower to report Connected=true")
	}
}

func TestReplicationMTLS_MutualAuthentication(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate CA/Server cert
	certPEM, keyPEM, err := replication.GenerateSelfSignedCert("localhost", "127.0.0.1")
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}

	certFile := filepath.Join(tmpDir, "mtls.crt")
	keyFile := filepath.Join(tmpDir, "mtls.key")
	_ = os.WriteFile(certFile, certPEM, 0o600)
	_ = os.WriteFile(keyFile, keyPEM, 0o600)

	// Server requires and verifies client certificate (mTLS)
	serverTLS, err := replication.NewServerTLSConfig(certFile, keyFile, certFile, true)
	if err != nil {
		t.Fatalf("server mtls config: %v", err)
	}

	primaryWAL, _ := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	defer primaryWAL.Close()

	primary, _ := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	defer primary.Close()

	replServer := replication.NewServer("127.0.0.1:0", primaryWAL)
	replServer.SetTLSConfig(serverTLS)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start repl server: %v", err)
	}
	defer replServer.Stop()

	// 1. Client WITHOUT client certificate must fail mTLS handshake
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(certPEM)
	noCertTLS := &tls.Config{RootCAs: caPool, MinVersion: tls.VersionTLS13}

	badConn, err := tls.Dial("tcp", replServer.Addr(), noCertTLS)
	if err == nil {
		// Attempting to read should result in handshake failure
		var buf [8]byte
		_, rerr := badConn.Read(buf[:])
		_ = badConn.Close()
		if rerr == nil {
			t.Fatalf("expected mTLS handshake failure without client cert")
		}
	}

	// 2. Follower WITH valid client certificate succeeds
	followerWAL, _ := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	defer followerWAL.Close()

	clientTLS, err := replication.NewClientTLSConfig(certFile, keyFile, certFile, false)
	if err != nil {
		t.Fatalf("client mtls config: %v", err)
	}

	follower := replication.NewFollower(replServer.Addr(), followerWAL)
	follower.SetTLSConfig(clientTLS)
	follower.Start()
	defer follower.Stop()

	ctx := context.Background()
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	_, _ = primary.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 1000}},
		{ID: accB, Currency: cur},
	})
	_, _ = primary.CreateTransfers(ctx, []core.Transfer{{
		ID:              mkID(1),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 100},
	}})

	for i := 0; i < 40; i++ {
		acc, err := follower.GetAccount(accA)
		if err == nil && acc.PostedDebits == (core.Uint128{Lo: 100}) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	acc, err := follower.GetAccount(accA)
	if err != nil || acc.PostedDebits != (core.Uint128{Lo: 100}) {
		t.Fatalf("mTLS follower failed to sync: %v, debits=%v", err, acc.PostedDebits)
	}
}
