package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"aequitas-ledger/bench"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/store/postgres"
	"aequitas-ledger/internal/wal"
)

func main() {
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	duration := flag.Duration("duration", 5*time.Second, "Duration of benchmark run per engine")
	numAccounts := flag.Int("accounts", 1000, "Number of accounts to generate")
	pgConn := flag.String("postgres", "", "PostgreSQL connection string (optional)")
	flag.Parse()

	fmt.Println("==========================================================================")
	fmt.Println(" ⚡ AEQUITAS LEDGER — BENCHMARK & BASELINE COMPARISON HARNESS ⚡")
	fmt.Println("==========================================================================")
	fmt.Printf(" Config: Concurrency=%d | Duration=%v | Accounts=%d\n\n", *concurrency, *duration, *numAccounts)

	accs := bench.GenerateAccounts(*numAccounts, 1_000_000_000_000)
	harness := bench.NewHarness(accs)

	// 1. Aequitas Ledger Benchmark
	fmt.Println("► Running Aequitas Ledger Engine Benchmark...")
	tmpDir, err := os.MkdirTemp("", "aequitas-bench-cli-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := wal.Open(filepath.Join(tmpDir, "wal"), 64<<20)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = accs

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		log.Fatalf("failed to initialize ledger: %v", err)
	}

	ctx := context.Background()
	aequitasRes := harness.Run(ctx, ledger, *concurrency, *duration)
	_ = ledger.Close()

	fmt.Printf("  • Aequitas Throughput : %.2f TPS\n", aequitasRes.TPS)
	fmt.Printf("  • Latency (p50 / p95 / p99): %v / %v / %v\n", aequitasRes.P50Latency, aequitasRes.P95Latency, aequitasRes.P99Latency)
	fmt.Printf("  • Errors              : %d\n\n", aequitasRes.ErrorCount)

	// 2. Postgres Baseline Benchmark (Optional)
	var pgRes bench.Result
	if *pgConn != "" {
		fmt.Println("► Running PostgreSQL Baseline Benchmark...")
		pgStore, err := postgres.NewStore(ctx, *pgConn)
		if err != nil {
			log.Printf("Failed to connect to Postgres (%v). Skipping Postgres run.\n", err)
		} else {
			defer pgStore.Close()
			_ = pgStore.Reset(ctx)
			for _, a := range accs {
				_ = pgStore.CreateAccount(ctx, a)
			}

			pgRes = harness.Run(ctx, pgStore, *concurrency, *duration)
			fmt.Printf("  • Postgres Throughput : %.2f TPS\n", pgRes.TPS)
			fmt.Printf("  • Latency (p50 / p95 / p99): %v / %v / %v\n", pgRes.P50Latency, pgRes.P95Latency, pgRes.P99Latency)
			fmt.Printf("  • Errors              : %d\n\n", pgRes.ErrorCount)
		}
	}

	fmt.Println("==========================================================================")
	fmt.Println(" ✅ BENCHMARK RUN COMPLETE")
	fmt.Println("==========================================================================")
}
