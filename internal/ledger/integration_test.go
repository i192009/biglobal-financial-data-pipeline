package ledger

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This test uses real PostgreSQL. CI invokes it inside the Compose network.
func TestPostgresConcurrentIdempotency(t *testing.T) {
	dsn := os.Getenv("TEST_SOURCE_DSN")
	if dsn == "" {
		t.Skip("TEST_SOURCE_DSN required for real PostgreSQL integration")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := Service{DB: db}
	key := uuid.NewString()
	var wg sync.WaitGroup
	results := make(chan json.RawMessage, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := service.Apply(ctx, "accounts", "", key, "integration", Request{Name: "Concurrent synthetic", Currency: "USD"})
			if err != nil {
				t.Error(err)
				return
			}
			results <- body
		}()
	}
	wg.Wait()
	close(results)
	var id string
	for body := range results {
		var row struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(body, &row); err != nil {
			t.Fatal(err)
		}
		if id != "" && id != row.ID {
			t.Fatal("idempotent retry created a second account")
		}
		id = row.ID
	}
	if id == "" {
		t.Fatal("no account created")
	}
	var linked int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM audit_events WHERE details->>'id'=$1 AND entity_id::text=$1", id).Scan(&linked); err != nil || linked != 1 {
		t.Fatalf("account audit is not linked to its entity: %d %v", linked, err)
	}
	depositKey := uuid.NewString()
	r := Request{AccountID: id, AmountMinor: 10000}
	for i := 0; i < 3; i++ {
		if _, err = service.Apply(ctx, "deposits", "", depositKey, "integration", r); err != nil {
			t.Fatal(err)
		}
	}
	var balance int64
	if err = db.QueryRow(ctx, "SELECT cash_minor FROM account_balances WHERE id=$1", id).Scan(&balance); err != nil || balance != 10000 {
		t.Fatalf("balance=%d err=%v", balance, err)
	}
	r.AmountMinor = 9999
	if _, err = service.Apply(ctx, "deposits", "", depositKey, "integration", r); err != ErrConflict {
		t.Fatalf("key conflict not rejected: %v", err)
	}
	r.AmountMinor = 10001
	if _, err = service.Apply(ctx, "withdrawals", "", uuid.NewString(), "integration", r); err == nil {
		t.Fatal("overdraft accepted")
	}
	if err = db.QueryRow(ctx, "SELECT cash_minor FROM account_balances WHERE id=$1", id).Scan(&balance); err != nil || balance != 10000 {
		t.Fatal("failed withdrawal changed balance")
	}
}

func TestPostgresExecutionCorrectionAndReversal(t *testing.T) {
	dsn := os.Getenv("TEST_SOURCE_DSN")
	if dsn == "" {
		t.Skip("TEST_SOURCE_DSN required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := Service{DB: db}
	apply := func(kind, id string, r Request) string {
		t.Helper()
		raw, err := s.Apply(ctx, kind, id, uuid.NewString(), "financial-integration", r)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		var result struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		return result.ID
	}
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	instrument := apply("instruments", "", Request{Name: "SYNTH-FIXTURE-" + uuid.NewString(), Currency: "USD"})
	account := apply("accounts", "", Request{Name: "Financial arithmetic fixture", Currency: "USD"})
	apply("deposits", "", Request{AccountID: account, AmountMinor: 1000000, OccurredAt: at})
	buyOrder := apply("orders", "", Request{AccountID: account, InstrumentID: instrument, Side: "buy", Quantity: 10})
	buy := apply("executions", "", Request{OrderID: buyOrder, ExecutionRef: uuid.NewString(), Quantity: 10, PriceMinor: 1000, FeeMinor: 100, OccurredAt: at})
	sellOrder := apply("orders", "", Request{AccountID: account, InstrumentID: instrument, Side: "sell", Quantity: 4})
	sell := apply("executions", "", Request{OrderID: sellOrder, ExecutionRef: uuid.NewString(), Quantity: 4, PriceMinor: 2000, FeeMinor: 50, OccurredAt: at.Add(time.Minute)})
	if _, err = s.Apply(ctx, "reverse", buy, uuid.NewString(), "financial-integration", Request{}); err == nil {
		t.Fatal("non-latest reversal accepted")
	}
	apply("reverse", sell, Request{OccurredAt: at.Add(2 * time.Minute)})
	corrected := apply("correct", buy, Request{ExecutionRef: uuid.NewString(), Quantity: 10, PriceMinor: 2000, FeeMinor: 100, OccurredAt: at})
	apply("executions", "", Request{OrderID: sellOrder, ExecutionRef: uuid.NewString(), Quantity: 4, PriceMinor: 3000, FeeMinor: 50, OccurredAt: at.Add(3 * time.Minute)})
	apply("withdrawals", "", Request{AccountID: account, AmountMinor: 100, OccurredAt: at.Add(4 * time.Minute)})
	apply("prices", "", Request{InstrumentID: instrument, PriceMinor: 2500, OccurredAt: at.Add(10 * time.Minute)})
	var cash, holding, ledger int64
	if err = db.QueryRow(ctx, "SELECT cash_minor FROM account_balances WHERE id=$1", account).Scan(&cash); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, "SELECT quantity FROM positions WHERE account_id=$1 AND instrument_id=$2", account, instrument).Scan(&holding); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, "SELECT sum(amount_minor)::bigint FROM account_transactions WHERE account_id=$1", account).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if cash != 991750 || holding != 6 || ledger != cash {
		t.Fatalf("financial effects differ: cash=%d holding=%d ledger=%d", cash, holding, ledger)
	}
	var fees int64
	if err = db.QueryRow(ctx, "SELECT sum(amount_minor)::bigint FROM fees WHERE account_id=$1 AND NOT reversed", account).Scan(&fees); err != nil || fees != 150 {
		t.Fatalf("fees=%d %v", fees, err)
	}
	t.Logf("account=%s corrected_execution=%s expected_cost_minor=12060 expected_realized_minor=3910 expected_unrealized_minor=2940", account, corrected)
}
