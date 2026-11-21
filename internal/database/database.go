package database

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func Context() (context.Context, context.CancelFunc) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func Open(ctx context.Context, key string) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(os.Getenv(key))
	if err != nil {
		panic(err)
	}
	cfg.MaxConns = 16
	cfg.MinConns = 1
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	if err = p.Ping(ctx); err != nil {
		panic(err)
	}
	return p
}
