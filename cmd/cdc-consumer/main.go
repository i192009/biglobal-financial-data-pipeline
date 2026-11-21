package main

import (
	"context"
	"github.com/biglobal/financial-data-pipeline/internal/cdc"
	"github.com/biglobal/financial-data-pipeline/internal/consumers"
	"github.com/biglobal/financial-data-pipeline/internal/database"
	"github.com/biglobal/financial-data-pipeline/internal/ledger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	ctx, cancel := database.Context()
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "replay-record" {
		if len(os.Args) != 5 {
			slog.Error("usage: cdc-consumer replay-record TOPIC PARTITION OFFSET")
			os.Exit(1)
		}
		partition, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			panic(err)
		}
		offset, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			panic(err)
		}
		timeout, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		appended, err := consumers.ReplayRecord(timeout, strings.Split(database.Env("BROKERS", "redpanda:9092"), ","), os.Args[2], int32(partition), offset)
		if err != nil {
			slog.Error("record_replay_failed", "error", err.Error())
			os.Exit(1)
		}
		slog.Info("original_record_replayed", "topic", os.Args[2], "partition", partition, "original_offset", offset, "appended_offset", appended)
		return
	}
	db := database.Open(ctx, "ANALYTICS_DSN")
	defer db.Close()
	processor := consumers.Processor{DB: db}
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		if err := processor.Replay(ctx); err != nil {
			slog.Error("replay_failed", "error", err.Error())
			os.Exit(1)
		}
		return
	}
	topics := make([]string, 0, len(cdc.Tables))
	for table := range cdc.Tables {
		topics = append(topics, "biglobal.trading."+table)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(database.Env("BROKERS", "redpanda:9092"), ",")...), kgo.ConsumerGroup(database.Env("CONSUMER_GROUP", "biglobal-analytics-v1")), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(8<<20), kgo.MetadataMaxAge(10*time.Second), kgo.SessionTimeout(30*time.Second), kgo.RebalanceTimeout(120*time.Second))
	if err != nil {
		panic(err)
	}
	defer client.CloseAllowingRebalance()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if db.Ping(r.Context()) != nil || client.Ping(r.Context()) != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	go ledger.Serve(ctx, ":8081", mux)
	if err = processor.Run(ctx, client, 4); err != nil && ctx.Err() == nil {
		slog.Error("consumer_stopped", "error", err.Error())
		os.Exit(1)
	}
}
