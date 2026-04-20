package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/observability"
	"aequitas-ledger/internal/wal"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"

	"google.golang.org/grpc"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	logFormat := os.Getenv("LOG_FORMAT")
	if logFormat == "" {
		logFormat = "text"
	}
	logger := observability.InitLogger(logLevel, logFormat)

	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	restPort := os.Getenv("REST_PORT")
	if restPort == "" {
		restPort = "8080"
	}
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "6060"
	}
	walDir := os.Getenv("WAL_DIR")
	if walDir == "" {
		walDir = filepath.Join(".", "data", "wal")
	}

	if err := os.MkdirAll(walDir, 0755); err != nil {
		logger.Error("failed to create wal directory", "error", err)
		os.Exit(1)
	}

	w, err := wal.Open(walDir, 64<<20) // 64MB segment size
	if err != nil {
		logger.Error("failed to open wal", "error", err)
		os.Exit(1)
	}

	cfg := engine.DefaultConfig()

	logger.Info("Initializing engine and executing WAL recovery...")
	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		_ = w.Close()
		logger.Error("failed to initialize ledger and recover WAL", "error", err)
		os.Exit(1)
	}
	logger.Info("Engine initialized and WAL recovery completed successfully.")

	// Start Observability / Metrics / pprof HTTP server
	obsServer := observability.NewServer(":" + metricsPort)
	go func() {
		logger.Info("Starting observability server (metrics/pprof)", "port", metricsPort)
		if err := obsServer.Start(); err != nil {
			logger.Error("observability server error", "error", err)
		}
	}()

	// Start REST API HTTP Gateway
	restServer := api.NewRESTServer(ledger)
	httpServer := &http.Server{
		Addr:         ":" + restPort,
		Handler:      restServer,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		logger.Info("Starting REST API HTTP Gateway", "port", restPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("REST HTTP server error", "error", err)
		}
	}()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		_ = ledger.Close()
		logger.Error("failed to listen on gRPC port", "port", port, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(api.IdempotencyUnaryInterceptor()),
	)
	handler := api.NewAccountsHandler(ledger)
	ledgerv1.RegisterLedgerServiceServer(grpcServer, handler)

	go func() {
		logger.Info("Starting gRPC server", "port", port)
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
