package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/config"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/observability"
	"aequitas-ledger/internal/replication"
	"aequitas-ledger/internal/wal"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"

	"google.golang.org/grpc"
)

func main() {
	cfg := config.Load()
	logger := observability.InitLogger(cfg.LogLevel, cfg.LogFormat)

	if err := os.MkdirAll(cfg.WALDir, 0755); err != nil {
		logger.Error("failed to create wal directory", "error", err)
		os.Exit(1)
	}

	w, err := wal.Open(cfg.WALDir, cfg.WALSegmentSize)
	if err != nil {
		logger.Error("failed to open wal", "error", err)
		os.Exit(1)
	}

	cfg.Engine.SnapshotDir = cfg.SnapshotDir
	cfg.Engine.IsFollower = (cfg.LedgerRole == "follower")
	cfg.Engine.SnapshotInterval = cfg.SnapshotInterval
	cfg.Engine.MaxSnapshotsKept = cfg.MaxSnapshotsKept

	logger.Info("Initializing engine and executing WAL recovery...")
	ledger, err := engine.NewLedger(cfg.Engine, w)
	if err != nil {
		_ = w.Close()
		logger.Error("failed to initialize ledger and recover WAL", "error", err)
		os.Exit(1)
	}
	logger.Info("Engine initialized and WAL recovery completed successfully.")

	// Start Replication based on LEDGER_ROLE
	if cfg.LedgerRole == "follower" {
		logger.Info("Node running in FOLLOWER (read-only) mode", "primary", cfg.PrimaryAddr)
		follower := replication.NewFollower(cfg.PrimaryAddr, w)
		follower.Start()
		defer follower.Stop()
	} else {
		logger.Info("Node running in PRIMARY mode", "replication_port", cfg.ReplicationPort)
		replServer := replication.NewServer(":"+cfg.ReplicationPort, w)
		if err := replServer.Start(); err != nil {
			logger.Error("failed to start replication listener", "error", err)
		} else {
			defer replServer.Stop()
		}
	}

	// Start Observability / Metrics / pprof HTTP server
	obsServer := observability.NewServer(":" + cfg.MetricsPort)
	go func() {
		logger.Info("Starting observability server (metrics/pprof)", "port", cfg.MetricsPort)
		if err := obsServer.Start(); err != nil {
			logger.Error("observability server error", "error", err)
		}
	}()

	// Start REST API HTTP Gateway
	restServer := api.NewRESTServer(ledger)
	httpServer := &http.Server{
		Addr:         ":" + cfg.RESTPort,
		Handler:      restServer,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		logger.Info("Starting REST API HTTP Gateway", "port", cfg.RESTPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("REST HTTP server error", "error", err)
		}
	}()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		_ = ledger.Close()
		logger.Error("failed to listen on gRPC port", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(api.IdempotencyUnaryInterceptor()),
	)
	handler := api.NewAccountsHandler(ledger)
	ledgerv1.RegisterLedgerServiceServer(grpcServer, handler)

	go func() {
		logger.Info("Starting gRPC server", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	logger.Info("Shutting down ledger server...")

	grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = obsServer.Shutdown(shutdownCtx)

	if err := ledger.Close(); err != nil {
		logger.Error("error closing ledger", "error", err)
	}
	fmt.Println("Server stopped cleanly.")
}
