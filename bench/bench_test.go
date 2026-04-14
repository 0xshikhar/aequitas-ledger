package bench

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/store/postgres"
	"aequitas-ledger/internal/wal"
)

func BenchmarkAequitasSequential(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "aequitas-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := wal.Open(filepath.Join(tmpDir, "wal"), 64<<20)
	if err != nil {
		b.Fatalf("failed to open wal: %v", err)
	}

	accs := GenerateAccounts(100, 1_000_000_000)
	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = accs

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		b.Fatalf("failed to create ledger: %v", err)
	}
	defer ledger.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		src := accs[i%50]
		dst := accs[50+(i%50)]

		var id [16]byte
		rand.Read(id[:])
		var key [32]byte
		rand.Read(key[:])

		tr := core.Transfer{
			ID:              id,
			DebitAccountID:  src.ID,
			CreditAccountID: dst.ID,
			Amount:          core.FromUint64(10),
			IdempotencyKey:  key,
		}

		_, err := ledger.CreateTransfer(ctx, tr)
		if err != nil {
			b.Fatalf("transfer failed: %v", err)
		}
	}
}

func BenchmarkAequitasConcurrent(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "aequitas-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := wal.Open(filepath.Join(tmpDir, "wal"), 64<<20)
	if err != nil {
		b.Fatalf("failed to open wal: %v", err)
	}

	accs := GenerateAccounts(1000, 1_000_000_000)
	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = accs

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		b.Fatalf("failed to create ledger: %v", err)
	}
	defer ledger.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		workerID := 0
		for pb.Next() {
			workerID++
			src := accs[workerID%500]
			dst := accs[500+(workerID%500)]

			var id [16]byte
			rand.Read(id[:])
			var key [32]byte
			rand.Read(key[:])

			tr := core.Transfer{
				ID:              id,
				DebitAccountID:  src.ID,
				CreditAccountID: dst.ID,
				Amount:          core.FromUint64(10),
				IdempotencyKey:  key,
			}

			_, _ = ledger.CreateTransfer(ctx, tr)
		}
	})
}

func BenchmarkPostgresBaseline(b *testing.B) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		b.Skip("Skipping Postgres baseline benchmark: DATABASE_URL not set")
	}

	ctx := context.Background()
	store, err := postgres.NewStore(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to connect to Postgres: %v", err)
	}
	defer store.Close()

	_ = store.Reset(ctx)
	accs := GenerateAccounts(100, 1_000_000_000)
	for _, acc := range accs {
		if err := store.CreateAccount(ctx, acc); err != nil {
			b.Fatalf("failed to seed account in postgres: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			idx++
			src := accs[idx%50]
			dst := accs[50+(idx%50)]

			var id [16]byte
			rand.Read(id[:])
			var key [32]byte
			rand.Read(key[:])

			tr := core.Transfer{
				ID:              id,
				DebitAccountID:  src.ID,
				CreditAccountID: dst.ID,
				Amount:          core.FromUint64(10),
				IdempotencyKey:  key,
			}

			_, _ = store.CreateTransfer(ctx, tr)
		}
	})
}
