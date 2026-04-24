package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupGRPCServer(tb testing.TB) (ledgerv1.LedgerServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	dir := tb.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		tb.Fatalf("failed to open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		tb.Fatalf("failed to start ledger: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(api.IdempotencyUnaryInterceptor()),
	)
	handler := api.NewAccountsHandler(ledger)
	ledgerv1.RegisterLedgerServiceServer(grpcServer, handler)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			tb.Errorf("grpc server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		tb.Fatalf("failed to dial bufnet: %v", err)
	}

	client := ledgerv1.NewLedgerServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
		_ = ledger.Close()
	}

	return client, cleanup
}

func TestGRPCAccountAndTransferRoundTrip(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create accounts via gRPC
	acc1ID := mkID(10)
	acc2ID := mkID(20)

	_, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id:                   acc1ID[:],
		Currency:             "USDC",
		InitialPostedCredits: &ledgerv1.Money{Lo: 100_000},
	})
	if err != nil {
		t.Fatalf("CreateAccount acc1 failed: %v", err)
	}

	_, err = client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id:                   acc2ID[:],
		Currency:             "USDC",
		InitialPostedCredits: &ledgerv1.Money{Lo: 50_000},
	})
	if err != nil {
		t.Fatalf("CreateAccount acc2 failed: %v", err)
	}

	// 2. Query accounts via gRPC
	getRes1, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc1ID[:]})
	if err != nil {
		t.Fatalf("GetAccount acc1 failed: %v", err)
	}
	if getRes1.Account.Balance.Lo != 100_000 {
		t.Fatalf("unexpected acc1 balance: got %d, expected 100000", getRes1.Account.Balance.Lo)
	}

	// 3. Create Transfer with Idempotency header via gRPC metadata
	mdCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", "grpc-tx-1"))
	txID := mkID(100)
	trRes, err := client.CreateTransfer(mdCtx, &ledgerv1.CreateTransferRequest{
		Id:              txID[:],
		DebitAccountId:  acc1ID[:],
		CreditAccountId: acc2ID[:],
		Amount:          &ledgerv1.Money{Lo: 25_000},
	})
	if err != nil {
		t.Fatalf("CreateTransfer failed: %v", err)
	}
	if trRes.Transfer.Amount.Lo != 25_000 {
		t.Fatalf("unexpected transfer amount response: %d", trRes.Transfer.Amount.Lo)
	}

	// 4. Verify balances updated
	acc1Updated, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc1ID[:]})
	if err != nil {
		t.Fatalf("GetAccount acc1 post-transfer failed: %v", err)
	}
	if acc1Updated.Account.Balance.Lo != 75_000 {
		t.Fatalf("unexpected acc1 post-transfer balance: %d", acc1Updated.Account.Balance.Lo)
	}

	acc2Updated, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc2ID[:]})
	if err != nil {
		t.Fatalf("GetAccount acc2 post-transfer failed: %v", err)
	}
	if acc2Updated.Account.Balance.Lo != 75_000 {
		t.Fatalf("unexpected acc2 post-transfer balance: %d", acc2Updated.Account.Balance.Lo)
	}
}

func TestGRPCConcurrentTransfersInvariant(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	acc1ID := mkID(1000)
	acc2ID := mkID(2000)

	initialMoney := uint64(10_000_000)
	_, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id:                   acc1ID[:],
		Currency:             "USD",
		InitialPostedCredits: &ledgerv1.Money{Lo: initialMoney},
	})
	if err != nil {
		t.Fatalf("failed to create acc1: %v", err)
	}

	_, err = client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id:                   acc2ID[:],
		Currency:             "USD",
		InitialPostedCredits: &ledgerv1.Money{Lo: 0},
	})
	if err != nil {
		t.Fatalf("failed to create acc2: %v", err)
	}

	goroutines := 50
	transfersPerRoutine := 100
	transferAmount := uint64(10)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < transfersPerRoutine; i++ {
				txNum := gIdx*transfersPerRoutine + i + 1
				txID := mkID(uint64(5000 + txNum))
				keyStr := fmt.Sprintf("goroutine-%d-tx-%d", gIdx, i)

				mdCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", keyStr))
				_, err := client.CreateTransfer(mdCtx, &ledgerv1.CreateTransferRequest{
					Id:              txID[:],
					DebitAccountId:  acc1ID[:],
					CreditAccountId: acc2ID[:],
					Amount:          &ledgerv1.Money{Lo: transferAmount},
				})
				if err != nil {
					t.Errorf("goroutine %d tx %d failed: %v", gIdx, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify final balances preserve invariant
	acc1Res, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc1ID[:]})
	if err != nil {
		t.Fatalf("GetAccount acc1 failed: %v", err)
	}
	acc2Res, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc2ID[:]})
	if err != nil {
		t.Fatalf("GetAccount acc2 failed: %v", err)
	}

	totalTransferred := uint64(goroutines * transfersPerRoutine * int(transferAmount))
	expectedAcc1 := initialMoney - totalTransferred
	expectedAcc2 := totalTransferred

	if acc1Res.Account.Balance.Lo != expectedAcc1 {
		t.Fatalf("acc1 balance mismatch: got %d, expected %d", acc1Res.Account.Balance.Lo, expectedAcc1)
	}
	if acc2Res.Account.Balance.Lo != expectedAcc2 {
		t.Fatalf("acc2 balance mismatch: got %d, expected %d", acc2Res.Account.Balance.Lo, expectedAcc2)
	}
	if acc1Res.Account.Balance.Lo+acc2Res.Account.Balance.Lo != initialMoney {
		t.Fatalf("invariant broken! total balance = %d", acc1Res.Account.Balance.Lo+acc2Res.Account.Balance.Lo)
	}
}
