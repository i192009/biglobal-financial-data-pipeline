package ledger

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/biglobal/financial-data-pipeline/internal/database"
	"github.com/biglobal/financial-data-pipeline/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func JSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func Auth(next http.Handler) http.Handler {
	token := database.Env("API_TOKEN", "local-synthetic-token")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			JSON(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
func Serve(ctx context.Context, addr string, handler http.Handler) {
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	slog.Info("http_listen", "address", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
func (s Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.DB.Ping(r.Context()); err != nil {
			JSON(w, 503, map[string]string{"status": "database unavailable"})
			return
		}
		JSON(w, 200, map[string]string{"status": "ok"})
	})
	for _, kind := range []string{"accounts", "orders", "executions", "deposits", "withdrawals", "prices", "instruments"} {
		mux.HandleFunc("POST /api/v1/"+kind, func(w http.ResponseWriter, r *http.Request) { s.mutate(w, r, kind, "") })
	}
	for _, kind := range []string{"reverse", "correct"} {
		mux.HandleFunc("POST /api/v1/executions/{id}/"+kind, func(w http.ResponseWriter, r *http.Request) { s.mutate(w, r, kind, r.PathValue("id")) })
	}
	mux.HandleFunc("GET /api/v1/accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		if _, err := uuid.Parse(r.PathValue("id")); err != nil {
			JSON(w, 400, map[string]string{"error": "invalid account identifier"})
			return
		}
		var data json.RawMessage
		err := s.DB.QueryRow(r.Context(), "SELECT jsonb_build_object('id',a.id,'customer_id',a.customer_id,'currency',a.currency,'status',a.status,'cash_minor',b.cash_minor) FROM accounts a JOIN account_balances b ON b.id=a.id WHERE a.id=$1", r.PathValue("id")).Scan(&data)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				JSON(w, 404, map[string]string{"error": "account not found"})
			} else {
				JSON(w, 503, map[string]string{"error": "account database unavailable"})
			}
			return
		}
		JSON(w, 200, map[string]any{"data": data})
	})
	for _, kind := range []string{"transactions", "trades", "instruments"} {
		mux.HandleFunc("GET /api/v1/"+kind, func(w http.ResponseWriter, r *http.Request) { s.list(w, r, kind) })
	}
	return Auth(mux)
}
func (s Service) mutate(w http.ResponseWriter, r *http.Request, kind, id string) {
	correlation := r.Header.Get("X-Correlation-ID")
	if len(correlation) == 0 || len(correlation) > 128 {
		correlation = uuid.NewString()
	}
	w.Header().Set("X-Correlation-ID", correlation)
	var req Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		JSON(w, 400, map[string]string{"error": "invalid JSON body"})
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		JSON(w, 400, map[string]string{"error": "one JSON object required"})
		return
	}
	if !req.OccurredAt.IsZero() && (req.OccurredAt.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) || req.OccurredAt.After(time.Now().Add(24*time.Hour))) {
		JSON(w, 400, map[string]string{"error": "timestamp outside synthetic reporting range"})
		return
	}
	response, err := s.Apply(r.Context(), kind, id, r.Header.Get("Idempotency-Key"), correlation, req)
	if err != nil {
		status := 400
		message := err.Error()
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			switch {
			case strings.HasPrefix(pgerr.Code, "08"), strings.HasPrefix(pgerr.Code, "53"), strings.HasPrefix(pgerr.Code, "57"), pgerr.Code == "40001", pgerr.Code == "40P01":
				status = 503
				message = "database temporarily unavailable; retry with the same idempotency key"
			case strings.HasPrefix(pgerr.Code, "23"):
				status = 409
				message = "operation violates a financial or reference constraint"
			case strings.HasPrefix(pgerr.Code, "22"):
				status = 400
				message = "invalid field value"
			default:
				status = 500
				message = "database operation failed"
			}
		}
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrDuplicate) {
			status = 409
		}
		if strings.Contains(message, "failed to connect") {
			status = 503
			message = "database unavailable"
		}
		observability.Requests.WithLabelValues(kind, strconv.Itoa(status)).Inc()
		slog.Warn("source_operation_failed", "route", kind, "correlation_id", correlation, "error", err.Error())
		JSON(w, status, map[string]string{"error": message})
		return
	}
	observability.Requests.WithLabelValues(kind, "201").Inc()
	JSON(w, 201, map[string]any{"data": response})
}
func (s Service) list(w http.ResponseWriter, r *http.Request, kind string) {
	table := map[string]string{"transactions": "account_transactions", "trades": "executions", "instruments": "instruments"}[kind]
	limit, offset := 100, 0
	if n, e := strconv.Atoi(r.URL.Query().Get("limit")); e == nil && n > 0 && n <= 1000 {
		limit = n
	}
	if n, e := strconv.Atoi(r.URL.Query().Get("offset")); e == nil && n >= 0 {
		offset = n
	}
	query := "SELECT to_jsonb(t) FROM " + table + " t"
	args := []any{limit, offset}
	if account := r.URL.Query().Get("account_id"); account != "" && kind != "instruments" {
		query += " WHERE account_id=$3"
		args = append(args, account)
	}
	query += " ORDER BY id LIMIT $1 OFFSET $2"
	rows, err := s.DB.Query(r.Context(), query, args...)
	if err != nil {
		JSON(w, 503, map[string]string{"error": "query failed"})
		return
	}
	defer rows.Close()
	data := []json.RawMessage{}
	for rows.Next() {
		var item json.RawMessage
		if err = rows.Scan(&item); err != nil {
			break
		}
		data = append(data, item)
	}
	if err != nil || rows.Err() != nil {
		JSON(w, 503, map[string]string{"error": "query failed"})
		return
	}
	JSON(w, 200, map[string]any{"data": data, "limit": limit, "offset": offset})
}
