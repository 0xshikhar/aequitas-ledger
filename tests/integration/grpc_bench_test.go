package integration

import (
	"context"
	"sync/atomic"
	"testing"

	ledgerv1 "aequitas-ledger/proto/ledger/v1"
)

// D1.1 proof benchmarks: end-to-end TPS through the gRPC surface, unary vs
// batch-8192. Run with:
//
//	go test -run '^$' -bench 'BenchmarkGRPC(Unary|Batch)' -benchtime 200x ./tests/integration
//
// Unary ns/op is per RPC (one transfer); batch ns/op is per RPC (8192
// transfers). Throughput ratio = (8192 * unary_ns) / batch_ns.
func benchGRPCFunded(b *testing.B) (ledgerv1.LedgerServiceClient, context.Context, [16]byte, [16]byte, func()) {
	client, cleanup := setupGRPCServer(b)
	ctx := context.Background()

	var acc1, acc2 [16]byte
	for i, id := range []uint64{1, 2} {
		acc := mkID(id)
		if i == 0 {
			acc1 = acc
		} else {
			acc2 = acc
		}
		if _, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
			Id:                   acc[:],
			Currency:             "USD",
			InitialPostedCredits: &ledgerv1.Money{Lo: 1 << 62}, // effectively unbounded for the bench
		}); err != nil {
			b.Fatalf("fund account: %v", err)
		}
	}
	return client, ctx, acc1, acc2, cleanup
}

func makeTransfer(n uint64, debit, credit [16]byte) *ledgerv1.Transfer {
	id := mkID(n)
	key := make([]byte, 32)
	for i := 0; i < 8; i++ {
		key[i] = byte(n >> (8 * i)) // unique per transfer: no dedup hits
	}
	return &ledgerv1.Transfer{
		Id:              id[:],
		DebitAccountId:  debit[:],
		CreditAccountId: credit[:],
		Amount:          &ledgerv1.Money{Lo: 1},
		IdempotencyKey:  key,
	}
}

// BenchmarkGRPCUnaryCreateTransfer is the "before" baseline: one durable
// transfer per RPC, one fsync per RPC-driven engine batch.
func BenchmarkGRPCUnaryCreateTransfer(b *testing.B) {
	client, ctx, debit, credit, cleanup := benchGRPCFunded(b)
	defer cleanup()

	var counter atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := makeTransfer(counter.Add(1), debit, credit)
		if _, err := client.CreateTransfer(ctx, &ledgerv1.CreateTransferRequest{
			Id:              tr.Id,
			DebitAccountId:  tr.DebitAccountId,
			CreditAccountId: tr.CreditAccountId,
			Amount:          tr.Amount,
			IdempotencyKey:  tr.IdempotencyKey,
		}); err != nil {
			b.Fatalf("unary transfer: %v", err)
		}
	}
}

// BenchmarkGRPCBatchCreateTransfers8192 is the "after": 8192 transfers per
// RPC, one shared completion, one fsync amortized across the batch.
func BenchmarkGRPCBatchCreateTransfers8192(b *testing.B) {
	client, ctx, debit, credit, cleanup := benchGRPCFunded(b)
	defer cleanup()

	const batchSize = 8192
	var counter atomic.Uint64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transfers := make([]*ledgerv1.Transfer, batchSize)
		base := counter.Add(batchSize)
		for k := range transfers {
			transfers[k] = makeTransfer(base-batchSize+uint64(k)+1, debit, credit)
		}
		res, err := client.CreateTransfers(ctx, &ledgerv1.CreateTransfersRequest{Transfers: transfers})
		if err != nil {
			b.Fatalf("batch transfer: %v", err)
		}
		for j := range res.Results {
			if !res.Results[j].Ok {
				b.Fatalf("item %d failed: %s", j, res.Results[j].Error)
			}
		}
	}
}
