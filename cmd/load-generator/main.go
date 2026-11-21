package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/biglobal/financial-data-pipeline/internal/database"
	"github.com/google/uuid"
)

type config struct {
	events, eps, accounts, instruments, batch, concurrency int
	seed                                                   uint64
	duplicate, correction, reversal, errors                float64
	start                                                  string
}
type api struct {
	base, token string
	client      *http.Client
	mu          sync.Mutex
	latencies   []time.Duration
	requests    atomic.Int64
}
type response struct {
	Data struct {
		ID string `json:"id"`
	} `json:"data"`
}

func (a *api) post(ctx context.Context, path, key string, payload any, expected int) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	started := time.Now()
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", a.base+"/api/v1/"+path, bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+a.token)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		res, err := a.client.Do(req)
		if err != nil {
			if attempt < 3 {
				continue
			}
			return "", err
		}
		raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		_ = res.Body.Close()
		a.requests.Add(1)
		if err != nil {
			return "", err
		}
		if res.StatusCode >= 500 && attempt < 3 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
			continue
		}
		if res.StatusCode != expected {
			return "", fmt.Errorf("%s returned %d (expected %d): %s", path, res.StatusCode, expected, string(raw))
		}
		var result response
		_ = json.Unmarshal(raw, &result)
		a.mu.Lock()
		a.latencies = append(a.latencies, time.Since(started))
		a.mu.Unlock()
		return result.Data.ID, nil
	}
	return "", fmt.Errorf("retry budget exhausted")
}
func main() {
	var c config
	flag.IntVar(&c.events, "events", 1000, "logical workload events; each may perform multiple source operations")
	flag.IntVar(&c.eps, "eps", 0, "global event rate (0 = unthrottled)")
	flag.IntVar(&c.accounts, "accounts", 10, "synthetic accounts (at least concurrency)")
	flag.IntVar(&c.instruments, "instruments", 2, "synthetic instruments")
	flag.IntVar(&c.batch, "batch-size", 100, "scheduler batch size; each HTTP mutation remains transactional")
	flag.IntVar(&c.concurrency, "concurrency", 4, "bounded workers")
	flag.Uint64Var(&c.seed, "seed", 42, "deterministic random seed (IDs/run prefix are unique)")
	flag.Float64Var(&c.duplicate, "duplicate-rate", .05, "same-key retry probability")
	flag.Float64Var(&c.correction, "correction-rate", .02, "immediate latest-execution correction probability")
	flag.Float64Var(&c.reversal, "reversal-rate", .02, "immediate latest-execution reversal probability")
	flag.Float64Var(&c.errors, "error-rate", .01, "rejected order injection probability")
	flag.StringVar(&c.start, "start-timestamp", time.Now().UTC().Add(-time.Hour).Format(time.RFC3339), "event-time baseline RFC3339")
	flag.Parse()
	if c.events < 0 || c.accounts < c.concurrency || c.concurrency < 1 || c.instruments < 1 || c.batch < 1 || c.eps < 0 {
		panic("invalid count/concurrency/batch/rate")
	}
	for _, rate := range []float64{c.duplicate, c.correction, c.reversal, c.errors} {
		if rate < 0 || rate > 1 {
			panic("rates must be 0..1")
		}
	}
	startAt, err := time.Parse(time.RFC3339, c.start)
	if err != nil {
		panic(err)
	}
	ctx, cancel := database.Context()
	defer cancel()
	run := uuid.NewString()
	a := &api{base: database.Env("API_URL", "http://localhost:8080"), token: database.Env("API_TOKEN", "local-synthetic-token"), client: &http.Client{Timeout: 30 * time.Second}}
	accounts := make([]string, c.accounts)
	instruments := make([]string, c.instruments)
	for i := range instruments {
		instruments[i], err = a.post(ctx, "instruments", fmt.Sprintf("%s-instrument-%d", run, i), map[string]any{"name": fmt.Sprintf("SYNTH-%s-%d", run, i), "currency": "USD"}, 201)
		if err != nil {
			panic(err)
		}
		_, err = a.post(ctx, "prices", fmt.Sprintf("%s-price-%d", run, i), map[string]any{"instrument_id": instruments[i], "price_minor": 11000 + i*100, "occurred_at": startAt}, 201)
		if err != nil {
			panic(err)
		}
	}
	for i := range accounts {
		accounts[i], err = a.post(ctx, "accounts", fmt.Sprintf("%s-account-%d", run, i), map[string]any{"name": fmt.Sprintf("Synthetic %s %d", run, i), "currency": "USD"}, 201)
		if err != nil {
			panic(err)
		}
		_, err = a.post(ctx, "deposits", fmt.Sprintf("%s-fund-%d", run, i), map[string]any{"account_id": accounts[i], "amount_minor": int64(1000000000000), "occurred_at": startAt}, 201)
		if err != nil {
			panic(err)
		}
	}
	began := time.Now()
	a.mu.Lock()
	a.latencies = nil
	a.mu.Unlock()
	a.requests.Store(0)
	queues := make([]chan int, c.concurrency)
	var wg sync.WaitGroup
	var first error
	var once sync.Once
	var completed atomic.Int64
	for worker := 0; worker < c.concurrency; worker++ {
		queues[worker] = make(chan int, c.batch)
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(c.seed, uint64(worker+1)))
			holdings := map[string]int64{}
			for index := range queues[worker] {
				if ctx.Err() != nil {
					return
				}
				account := accounts[index%c.accounts]
				instrument := instruments[rng.IntN(c.instruments)]
				holdingKey := account + instrument
				key := fmt.Sprintf("%s-event-%d", run, index)
				at := startAt.Add(time.Duration(index) * time.Millisecond)
				var eventErr error
				if rng.Float64() < .10 {
					kind := "deposits"
					if rng.IntN(2) == 0 {
						kind = "withdrawals"
					}
					payload := map[string]any{"account_id": account, "amount_minor": 1000, "occurred_at": at}
					_, eventErr = a.post(ctx, kind, key, payload, 201)
					if eventErr == nil && rng.Float64() < c.duplicate {
						_, eventErr = a.post(ctx, kind, key, payload, 201)
					}
				} else {
					qty := int64(rng.IntN(10) + 1)
					side := "buy"
					if holdings[holdingKey] >= qty && rng.IntN(2) == 0 {
						side = "sell"
					}
					if rng.Float64() < c.errors {
						_, eventErr = a.post(ctx, "orders", key+"-invalid", map[string]any{"account_id": account, "instrument_id": instrument, "side": "INVALID", "quantity": qty}, 400)
					}
					var order string
					if eventErr == nil {
						order, eventErr = a.post(ctx, "orders", key+"-order", map[string]any{"account_id": account, "instrument_id": instrument, "side": side, "quantity": qty}, 201)
					}
					payload := map[string]any{"order_id": order, "execution_ref": key, "quantity": qty, "price_minor": 10000 + rng.IntN(1000), "fee_minor": 10, "occurred_at": at}
					var execution string
					if eventErr == nil {
						execution, eventErr = a.post(ctx, "executions", key, payload, 201)
					}
					if eventErr == nil && rng.Float64() < c.duplicate {
						_, eventErr = a.post(ctx, "executions", key, payload, 201)
					}
					if eventErr == nil {
						delta := qty
						if side == "sell" {
							delta = -delta
						}
						holdings[holdingKey] += delta
						if rng.Float64() < c.reversal {
							_, eventErr = a.post(ctx, "executions/"+execution+"/reverse", key+"-reverse", map[string]any{"occurred_at": at.Add(time.Nanosecond)}, 201)
							if eventErr == nil {
								holdings[holdingKey] -= delta
							}
						} else if rng.Float64() < c.correction {
							payload["execution_ref"] = key + "-corrected"
							payload["price_minor"] = 10500
							payload["occurred_at"] = at.Add(time.Nanosecond)
							_, eventErr = a.post(ctx, "executions/"+execution+"/correct", key+"-correct", payload, 201)
						}
					}
				}
				if eventErr != nil {
					once.Do(func() { first = eventErr; cancel() })
					return
				}
				completed.Add(1)
			}
		}(worker)
	}
	// Account affinity keeps each local holdings map correct while allowing parallel accounts.
schedule:
	for base := 0; base < c.events; base += c.batch {
		end := min(c.events, base+c.batch)
		for i := base; i < end; i++ {
			if c.eps > 0 {
				delay := time.Until(began.Add(time.Duration(i) * time.Second / time.Duration(c.eps)))
				if delay > 0 {
					select {
					case <-ctx.Done():
						break schedule
					case <-time.After(delay):
					}
				}
			}
			worker := (i % c.accounts) % c.concurrency
			select {
			case queues[worker] <- i:
			case <-ctx.Done():
				break schedule
			}
		}
	}
	for _, queue := range queues {
		close(queue)
	}
	wg.Wait()
	elapsed := time.Since(began)
	sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
	percentile := func(p float64) float64 {
		if len(a.latencies) == 0 {
			return 0
		}
		return float64(a.latencies[int(float64(len(a.latencies)-1)*p)].Microseconds()) / 1000
	}
	report := map[string]any{"run_id": run, "seed": c.seed, "requested_events": c.events, "completed_events": completed.Load(), "source_http_requests": a.requests.Load(), "source_duration_seconds": elapsed.Seconds(), "source_requests_per_second": float64(a.requests.Load()) / elapsed.Seconds(), "source_http_p50_ms": percentile(.50), "source_http_p95_ms": percentile(.95), "source_http_p99_ms": percentile(.99), "concurrency": c.concurrency, "accounts": c.accounts, "instruments": c.instruments, "error": nil}
	if first != nil {
		report["error"] = first.Error()
	}
	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))
	slog.Info("generator_finished", "completed_events", completed.Load())
	if first != nil || completed.Load() != int64(c.events) {
		os.Exit(1)
	}
}
