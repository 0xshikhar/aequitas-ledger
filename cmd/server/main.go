package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func main() {
	walDir := os.Getenv("WAL_DIR")
	if walDir == "" {
		walDir = filepath.Join(".", "data", "wal")
	}

	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("failed to create wal directory: %v", err)
	}

	w, err := wal.Open(walDir, 64<<20) // 64MB segment size
	if err != nil {
		log.Fatalf("failed to open wal: %v", err)
	}

	cfg := engine.DefaultConfig()

	log.Println("Initializing engine and executing WAL recovery...")
	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to initialize ledger and recover WAL: %v", err)
	}
	log.Println("Engine initialized and WAL recovery complete successfully.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	log.Println("Shutting down ledger server...")

	if err := ledger.Close(); err != nil {
		log.Printf("error closing ledger: %v", err)
	}
	fmt.Println("Server stopped cleanly.")
}
