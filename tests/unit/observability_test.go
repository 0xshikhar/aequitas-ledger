package unit

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"aequitas-ledger/internal/observability"
)

func TestObservabilityServer(t *testing.T) {
	addr := "127.0.0.1:16060"
	server := observability.NewServer(addr)

	go func() {
		_ = server.Start()
	}()
	time.Sleep(50 * time.Millisecond)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Test /metrics endpoint
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to fetch /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK for /metrics, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Errorf("expected non-empty metrics response body")
	}

	// Test /debug/pprof/ endpoint
	pprofResp, err := http.Get("http://" + addr + "/debug/pprof/")
	if err != nil {
		t.Fatalf("failed to fetch /debug/pprof/: %v", err)
	}
	defer pprofResp.Body.Close()

	if pprofResp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK for /debug/pprof/, got %d", pprofResp.StatusCode)
	}
}
