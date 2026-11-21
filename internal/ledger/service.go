package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/biglobal/financial-data-pipeline/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Request struct {
	Name         string    `json:"name"`
	Currency     string    `json:"currency"`
	AccountID    string    `json:"account_id"`
	InstrumentID string    `json:"instrument_id"`
	OrderID      string    `json:"order_id"`
	ExecutionRef string    `json:"execution_ref"`
	Side         string    `json:"side"`
	Quantity     int64     `json:"quantity"`
	PriceMinor   int64     `json:"price_minor"`
	FeeMinor     int64     `json:"fee_minor"`
	AmountMinor  int64     `json:"amount_minor"`
	OccurredAt   time.Time `json:"occurred_at"`
}
type Service struct{ DB *pgxpool.Pool }

var ErrConflict = errors.New("idempotency key reused with different request")
var ErrDuplicate = errors.New("execution reference already exists")

func (s Service) Apply(ctx context.Context, kind, id, key, correlation string, r Request) (json.RawMessage, error) {
	start := time.Now()
	defer func() { observability.Transactions.Observe(time.Since(start).Seconds()) }()
	if len(key) < 1 || len(key) > 200 {
		return nil, errors.New("Idempotency-Key of 1..200 characters required")
	}
	// Fingerprint the supplied timestamp, before applying the default.
	b, _ := json.Marshal(r)
	hash := sha256.Sum256(append([]byte(kind+id), b...))
	digest := hex.EncodeToString(hash[:])
	if r.OccurredAt.IsZero() {
		r.OccurredAt = time.Now().UTC()
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "INSERT INTO idempotency_keys(key,request_hash) VALUES($1,$2) ON CONFLICT DO NOTHING", key, digest)
	if err != nil {
		return nil, err
	}
	var stored string
	var response json.RawMessage
	err = tx.QueryRow(ctx, "SELECT request_hash,response FROM idempotency_keys WHERE key=$1 FOR UPDATE", key).Scan(&stored, &response)
	if err != nil {
		return nil, err
	}
	if stored != digest {
		return nil, ErrConflict
	}
	if len(response) > 0 {
		return response, tx.Commit(ctx)
	}
	var result any
	switch kind {
	case "accounts":
		result, err = createAccount(ctx, tx, r)
	case "orders":
		result, err = createOrder(ctx, tx, r)
	case "executions":
		result, err = execute(ctx, tx, r)
	case "deposits", "withdrawals":
		result, err = cash(ctx, tx, r, kind)
	case "prices":
		result, err = price(ctx, tx, r)
	case "instruments":
		result, err = instrument(ctx, tx, r)
	case "reverse":
		result, err = reverse(ctx, tx, id, r.OccurredAt)
	case "correct":
		var order string
		err = tx.QueryRow(ctx, "SELECT order_id::text FROM executions WHERE id=$1", id).Scan(&order)
		if err == nil {
			_, err = reverse(ctx, tx, id, r.OccurredAt)
		}
		if err == nil {
			r.OrderID = order
			result, err = execute(ctx, tx, r)
		}
	default:
		err = errors.New("unsupported operation")
	}
	if err != nil {
		return nil, err
	}
	response, err = json.Marshal(result)
	if err != nil {
		return nil, err
	}
	var entity struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(response, &entity); err != nil {
		return nil, err
	}
	if _, err = uuid.Parse(entity.ID); err != nil {
		return nil, errors.New("operation returned an invalid audit entity identifier")
	}
	_, err = tx.Exec(ctx, "INSERT INTO audit_events(action,entity_id,correlation_id,details) VALUES($1,$2,$3,$4)", kind, entity.ID, correlation, response)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "UPDATE idempotency_keys SET response=$2 WHERE key=$1", key, response)
	if err != nil {
		return nil, err
	}
	return response, tx.Commit(ctx)
}
func createAccount(ctx context.Context, tx pgx.Tx, r Request) (any, error) {
	if len(r.Name) == 0 || len(r.Name) > 120 {
		return nil, errors.New("name required (max 120)")
	}
	if r.Currency != "USD" && r.Currency != "GBP" && r.Currency != "EUR" {
		return nil, errors.New("unsupported currency")
	}
	customer, id := uuid.NewString(), uuid.NewString()
	if _, err := tx.Exec(ctx, "INSERT INTO customers(id,name) VALUES($1,$2)", customer, r.Name); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO accounts(id,customer_id,currency) VALUES($1,$2,$3)", id, customer, r.Currency); err != nil {
		return nil, err
	}
	_, err := tx.Exec(ctx, "INSERT INTO account_balances(id) VALUES($1)", id)
	return map[string]any{"id": id, "customer_id": customer, "currency": r.Currency}, err
}
func createOrder(ctx context.Context, tx pgx.Tx, r Request) (any, error) {
	if r.Side != "buy" && r.Side != "sell" {
		return nil, errors.New("invalid side")
	}
	if r.Quantity < 1 || r.Quantity > 1000000 {
		return nil, errors.New("invalid quantity")
	}
	var valid bool
	err := tx.QueryRow(ctx, "SELECT a.currency=i.currency AND a.status='active' FROM accounts a CROSS JOIN instruments i WHERE a.id=$1 AND i.id=$2", r.AccountID, r.InstrumentID).Scan(&valid)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New("inactive account or instrument currency mismatch")
	}
	id := uuid.NewString()
	_, err = tx.Exec(ctx, "INSERT INTO orders(id,account_id,instrument_id,side,quantity) VALUES($1,$2,$3,$4,$5)", id, r.AccountID, r.InstrumentID, r.Side, r.Quantity)
	return map[string]any{"id": id}, err
}
func lockAccount(ctx context.Context, tx pgx.Tx, id string) (int64, string, error) {
	var balance int64
	var currency string
	err := tx.QueryRow(ctx, "SELECT b.cash_minor,a.currency FROM account_balances b JOIN accounts a ON a.id=b.id WHERE b.id=$1 AND a.status='active' FOR UPDATE OF b", id).Scan(&balance, &currency)
	return balance, currency, err
}
func cash(ctx context.Context, tx pgx.Tx, r Request, kind string) (any, error) {
	if r.AmountMinor < 1 || r.AmountMinor > 1000000000000 {
		return nil, errors.New("amount must be 1..1000000000000 minor units")
	}
	balance, currency, err := lockAccount(ctx, tx, r.AccountID)
	if err != nil {
		return nil, err
	}
	amount := r.AmountMinor
	if kind == "withdrawals" {
		amount = -amount
	}
	if balance+amount < 0 {
		return nil, errors.New("insufficient cash")
	}
	id, transaction := uuid.NewString(), uuid.NewString()
	singular := "deposit"
	if kind == "withdrawals" {
		singular = "withdrawal"
	}
	_, err = tx.Exec(ctx, "INSERT INTO account_transactions(id,account_id,kind,amount_minor,currency,occurred_at) VALUES($1,$2,$3,$4,$5,$6)", transaction, r.AccountID, singular, amount, currency, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	// kind is an internal allowlist, never user SQL.
	_, err = tx.Exec(ctx, "INSERT INTO "+kind+"(id,account_id,transaction_id,amount_minor,occurred_at) VALUES($1,$2,$3,$4,$5)", id, r.AccountID, transaction, r.AmountMinor, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "UPDATE account_balances SET cash_minor=cash_minor+$2,updated_at=now() WHERE id=$1", r.AccountID, amount)
	return map[string]any{"id": id, "transaction_id": transaction, "cash_minor": balance + amount}, err
}
func execute(ctx context.Context, tx pgx.Tx, r Request) (any, error) {
	notional, err := Notional(r.Quantity, r.PriceMinor, r.FeeMinor)
	if err != nil {
		return nil, err
	}
	if len(r.ExecutionRef) < 1 || len(r.ExecutionRef) > 160 {
		return nil, errors.New("execution_ref required (max 160)")
	}
	var account, instrument, side string
	var ordered int64
	err = tx.QueryRow(ctx, "SELECT account_id::text,instrument_id::text,side,quantity FROM orders WHERE id=$1", r.OrderID).Scan(&account, &instrument, &side, &ordered)
	if err != nil {
		return nil, err
	}
	balance, currency, err := lockAccount(ctx, tx, account)
	if err != nil {
		return nil, err
	}
	var later bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM executions WHERE account_id=$1 AND instrument_id=$2 AND NOT reversed AND occurred_at>$3)", account, instrument, r.OccurredAt).Scan(&later); err != nil {
		return nil, err
	}
	if later {
		return nil, errors.New("execution timestamp precedes an active execution for this account/instrument")
	}
	var duplicate bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM executions WHERE execution_ref=$1)", r.ExecutionRef).Scan(&duplicate); err != nil {
		return nil, err
	}
	if duplicate {
		return nil, ErrDuplicate
	}
	var filled int64
	err = tx.QueryRow(ctx, "SELECT coalesce(sum(quantity),0)::bigint FROM executions WHERE order_id=$1 AND NOT reversed", r.OrderID).Scan(&filled)
	if err != nil {
		return nil, err
	}
	if filled+r.Quantity > ordered {
		return nil, errors.New("execution exceeds unfilled order quantity")
	}
	_, err = tx.Exec(ctx, "INSERT INTO positions(account_id,instrument_id,quantity) VALUES($1,$2,0) ON CONFLICT DO NOTHING", account, instrument)
	if err != nil {
		return nil, err
	}
	var holding int64
	err = tx.QueryRow(ctx, "SELECT quantity FROM positions WHERE account_id=$1 AND instrument_id=$2", account, instrument).Scan(&holding)
	if err != nil {
		return nil, err
	}
	movement, amount := r.Quantity, -notional-r.FeeMinor
	if side == "sell" {
		movement = -movement
		amount = notional - r.FeeMinor
	}
	if balance+amount < 0 || holding+movement < 0 {
		return nil, errors.New("insufficient cash or long position")
	}
	id := uuid.NewString()
	_, err = tx.Exec(ctx, `INSERT INTO executions(id,execution_ref,order_id,account_id,instrument_id,side,quantity,price_minor,notional_minor,currency,fee_minor,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, id, r.ExecutionRef, r.OrderID, account, instrument, side, r.Quantity, r.PriceMinor, notional, currency, r.FeeMinor, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO account_transactions(account_id,kind,amount_minor,currency,execution_id,occurred_at) VALUES($1,'execution',$2,$3,$4,$5)", account, amount, currency, id, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO fees(execution_id,account_id,amount_minor,currency,occurred_at) VALUES($1,$2,$3,$4,$5)", id, account, r.FeeMinor, currency, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO position_movements(account_id,instrument_id,execution_id,quantity,occurred_at) VALUES($1,$2,$3,$4,$5)", account, instrument, id, movement, r.OccurredAt)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "UPDATE positions SET quantity=quantity+$3 WHERE account_id=$1 AND instrument_id=$2", account, instrument, movement); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "UPDATE account_balances SET cash_minor=cash_minor+$2,updated_at=now() WHERE id=$1", account, amount)
	return map[string]any{"id": id, "execution_ref": r.ExecutionRef, "notional_minor": notional}, err
}
func reverse(ctx context.Context, tx pgx.Tx, id string, at time.Time) (any, error) {
	var account, instrument string
	var sequence int64
	var occurred time.Time
	err := tx.QueryRow(ctx, "SELECT account_id::text,instrument_id::text,sequence_id,occurred_at FROM executions WHERE id=$1 AND NOT reversed", id).Scan(&account, &instrument, &sequence, &occurred)
	if err != nil {
		return nil, err
	}
	if at.Before(occurred) {
		return nil, errors.New("reversal timestamp precedes execution")
	}
	balance, _, err := lockAccount(ctx, tx, account)
	if err != nil {
		return nil, err
	}
	var newer bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM executions WHERE account_id=$1 AND instrument_id=$2 AND sequence_id>$3 AND NOT reversed)", account, instrument, sequence).Scan(&newer)
	if err != nil {
		return nil, err
	}
	if newer {
		return nil, errors.New("only latest active execution for account/instrument can be reversed")
	}
	var transaction string
	var amount, movement, holding int64
	var currency string
	err = tx.QueryRow(ctx, "SELECT id::text,amount_minor,currency FROM account_transactions WHERE execution_id=$1 AND kind='execution'", id).Scan(&transaction, &amount, &currency)
	if err != nil {
		return nil, err
	}
	err = tx.QueryRow(ctx, "SELECT quantity FROM position_movements WHERE execution_id=$1 AND NOT is_reversal", id).Scan(&movement)
	if err != nil {
		return nil, err
	}
	err = tx.QueryRow(ctx, "SELECT quantity FROM positions WHERE account_id=$1 AND instrument_id=$2", account, instrument).Scan(&holding)
	if err != nil {
		return nil, err
	}
	if balance-amount < 0 || holding-movement < 0 {
		return nil, errors.New("reversal would violate balance or position constraint")
	}
	_, err = tx.Exec(ctx, "INSERT INTO account_transactions(account_id,kind,amount_minor,currency,execution_id,reversal_of,occurred_at) VALUES($1,'reversal',$2,$3,$4,$5,$6)", account, -amount, currency, id, transaction, at)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO position_movements(account_id,instrument_id,execution_id,quantity,is_reversal,occurred_at) VALUES($1,$2,$3,$4,true,$5)", account, instrument, id, -movement, at)
	if err != nil {
		return nil, err
	}
	for _, q := range []string{"UPDATE executions SET reversed=true WHERE id=$1", "UPDATE fees SET reversed=true WHERE execution_id=$1"} {
		if _, err = tx.Exec(ctx, q, id); err != nil {
			return nil, err
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE account_balances SET cash_minor=cash_minor-$2,updated_at=now() WHERE id=$1", account, amount); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "UPDATE positions SET quantity=quantity-$3 WHERE account_id=$1 AND instrument_id=$2", account, instrument, movement)
	return map[string]any{"id": id, "reversed": true}, err
}
func price(ctx context.Context, tx pgx.Tx, r Request) (any, error) {
	if r.PriceMinor < 1 || r.PriceMinor > 1000000000 {
		return nil, errors.New("invalid price")
	}
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, "INSERT INTO price_snapshots(id,instrument_id,price_minor,currency,occurred_at) SELECT $1,id,$2,currency,$3 FROM instruments WHERE id=$4", id, r.PriceMinor, r.OccurredAt, r.InstrumentID)
	if err == nil && tag.RowsAffected() != 1 {
		err = fmt.Errorf("unknown instrument")
	}
	return map[string]any{"id": id}, err
}
func instrument(ctx context.Context, tx pgx.Tx, r Request) (any, error) {
	if len(r.Name) < 1 || len(r.Name) > 80 {
		return nil, errors.New("synthetic symbol required (max 80)")
	}
	id := uuid.NewString()
	_, err := tx.Exec(ctx, "INSERT INTO instruments(id,symbol,currency) VALUES($1,$2,$3)", id, r.Name, r.Currency)
	return map[string]any{"id": id, "symbol": r.Name}, err
}
