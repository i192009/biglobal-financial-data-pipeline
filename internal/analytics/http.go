package analytics

import (
	"encoding/json"
	"github.com/biglobal/financial-data-pipeline/internal/ledger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"strconv"
	"time"
)

type Service struct{ DB *pgxpool.Pool }
type report struct {
	query         string
	account, date bool
}

func (s Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.DB.Ping(r.Context()) != nil {
			ledger.JSON(w, 503, map[string]string{"status": "database unavailable"})
			return
		}
		ledger.JSON(w, 200, map[string]string{"status": "ok"})
	})
	reports := map[string]report{
		"overview":        {query: "SELECT currency,sum(executions) AS executions_processed,sum(trade_volume_minor) AS trade_volume_minor,sum(fees_minor) AS fees_minor FROM analytics.mart_daily_trading_summary GROUP BY currency"},
		"trades":          {query: "SELECT * FROM analytics.fct_trade_executions WHERE true", account: true, date: true},
		"accounts":        {query: "SELECT a.*,coalesce(c.ledger_cash_minor,0) AS cash_minor FROM analytics.stg_accounts a LEFT JOIN analytics.int_reconciliation_inputs c ON c.account_id=a.id WHERE true", account: true},
		"positions":       {query: "SELECT * FROM analytics.fct_daily_positions WHERE true", account: true, date: true},
		"pnl":             {query: "SELECT * FROM analytics.mart_account_profitability WHERE true", account: true},
		"cashflows":       {query: "SELECT * FROM analytics.fct_account_cash_flows WHERE true", account: true, date: true},
		"exposure":        {query: "SELECT * FROM analytics.mart_portfolio_exposure WHERE true", account: true, date: true},
		"reconciliation":  {query: "SELECT * FROM operations.reconciliation_results WHERE true"},
		"pipeline-health": {query: "SELECT (SELECT count(*) FROM raw.processing_failures WHERE resolved_at IS NULL) AS unresolved_failures,(SELECT max(ingested_at) FROM raw.raw_cdc_events) AS last_ingested_at,(SELECT count(*) FROM raw.raw_cdc_events) AS captured_events,(SELECT success FROM operations.dbt_runs ORDER BY created_at DESC LIMIT 1) AS last_dbt_success"},
	}
	for name, rep := range reports {
		mux.HandleFunc("GET /analytics/"+name, func(w http.ResponseWriter, r *http.Request) { s.read(w, r, name, rep) })
	}
	return ledger.Auth(mux)
}
func (s Service) read(w http.ResponseWriter, r *http.Request, name string, rep report) {
	values := r.URL.Query()
	limit, offset := 100, 0
	if str := values.Get("limit"); str != "" {
		v, err := strconv.Atoi(str)
		if err != nil || v < 1 || v > 1000 {
			ledger.JSON(w, 400, map[string]string{"error": "limit must be 1..1000"})
			return
		}
		limit = v
	}
	if str := values.Get("offset"); str != "" {
		v, err := strconv.Atoi(str)
		if err != nil || v < 0 {
			ledger.JSON(w, 400, map[string]string{"error": "offset must be nonnegative"})
			return
		}
		offset = v
	}
	query := rep.query
	args := []any{}
	if id := values.Get("account_id"); id != "" && rep.account {
		args = append(args, id)
		col := "account_id"
		if name == "accounts" {
			col = "a.id"
		}
		query += " AND " + col + "=$" + strconv.Itoa(len(args))
	}
	if rep.date {
		for _, item := range []struct{ key, op string }{{"from", ">="}, {"to", "<"}} {
			if str := values.Get(item.key); str != "" {
				at, err := time.Parse(time.RFC3339, str)
				if err != nil {
					ledger.JSON(w, 400, map[string]string{"error": "date filters require RFC3339 timestamps; to is exclusive"})
					return
				}
				args = append(args, at)
				col := "occurred_at"
				if name == "positions" || name == "exposure" {
					col = "reporting_date"
				}
				query += " AND " + col + item.op + "$" + strconv.Itoa(len(args))
			}
		}
	}
	// Stable ordering makes offset pagination reproducible at a fixed dbt version.
	order := map[string]string{"overview": "currency", "trades": "sequence_id", "accounts": "a.id", "positions": "reporting_date,account_id,instrument_id", "pnl": "account_id", "cashflows": "sequence_id", "exposure": "account_id,instrument_id", "reconciliation": "created_at DESC,id"}[name]
	if order != "" {
		query += " ORDER BY " + order
	}
	args = append(args, limit, offset)
	query = "SELECT to_jsonb(report_row) FROM (" + query + " LIMIT $" + strconv.Itoa(len(args)-1) + " OFFSET $" + strconv.Itoa(len(args)) + ") report_row"
	rows, err := s.DB.Query(r.Context(), query, args...)
	if err != nil {
		ledger.JSON(w, 503, map[string]string{"error": "analytics unavailable; run dbt build and inspect pipeline health"})
		return
	}
	defer rows.Close()
	data := []json.RawMessage{}
	for rows.Next() {
		var raw json.RawMessage
		if err = rows.Scan(&raw); err != nil {
			break
		}
		data = append(data, raw)
	}
	if err != nil || rows.Err() != nil {
		ledger.JSON(w, 503, map[string]string{"error": "analytics query failed"})
		return
	}
	ledger.JSON(w, 200, map[string]any{"data": data, "limit": limit, "offset": offset, "units": "minor currency units; numeric fractional cost/P&L; currencies reported separately"})
}
