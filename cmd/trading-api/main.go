package main

import (
	"github.com/biglobal/financial-data-pipeline/internal/database"
	"github.com/biglobal/financial-data-pipeline/internal/ledger"
)

func main() {
	ctx, cancel := database.Context()
	defer cancel()
	db := database.Open(ctx, "SOURCE_DSN")
	defer db.Close()
	ledger.Serve(ctx, database.Env("LISTEN_ADDR", ":8080"), ledger.Service{DB: db}.Handler())
}
