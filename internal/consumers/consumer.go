package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/biglobal/financial-data-pipeline/internal/cdc"
	"github.com/biglobal/financial-data-pipeline/internal/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Processor struct{ DB *pgxpool.Pool }

// Persist commits the history, normalized state (or quarantine), and checkpoint atomically.
func (p Processor) Persist(ctx context.Context, r *kgo.Record) error {
	return p.PersistBatch(ctx, []*kgo.Record{r})
}

// Fixed workers execute whole partition batches sequentially. Polls are bounded and
// rebalances are blocked only until the fetched batch has reached durable storage.
func (p Processor) Run(ctx context.Context, client *kgo.Client, workers int) error {
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 500)
		if errs := fetches.Errors(); len(errs) > 0 {
			slog.Warn("kafka_fetch", "error", errs[0].Err.Error())
			if ctx.Err() != nil {
				client.AllowRebalance()
				break
			}
			// Successful partitions in the same fetch still require persistence.
			if fetches.NumRecords() == 0 {
				client.AllowRebalance()
				continue
			}
		}
		jobs := make(chan []*kgo.Record)
		batchCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		var first error
		var once sync.Once
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for records := range jobs {
					if len(records) == 0 {
						continue
					}
					r := records[0]
					var err error
					for attempt := 0; attempt < 7; attempt++ {
						attemptCtx, stop := context.WithTimeout(batchCtx, 10*time.Second)
						err = p.PersistBatch(attemptCtx, records)
						stop()
						if err == nil {
							break
						}
						observability.Retries.Inc()
						slog.Warn("cdc_persist_retry", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err.Error())
						select {
						case <-batchCtx.Done():
						case <-time.After(time.Duration(1<<attempt) * 100 * time.Millisecond):
						}
						if batchCtx.Err() != nil {
							break
						}
					}
					if err != nil {
						once.Do(func() { first = err; cancel() })
						return
					}
				}
			}()
		}
		fetches.EachPartition(func(part kgo.FetchTopicPartition) {
			select {
			case jobs <- part.Records:
			case <-batchCtx.Done():
			}
		})
		close(jobs)
		wg.Wait()
		cancel()
		if first != nil {
			client.AllowRebalance()
			return first
		}
		// Kafka acknowledgement occurs strictly after every required DB commit.
		var ends []*kgo.Record
		fetches.EachPartition(func(part kgo.FetchTopicPartition) {
			if len(part.Records) > 0 {
				ends = append(ends, part.Records[len(part.Records)-1])
			}
		})
		err := client.CommitRecords(ctx, ends...)
		client.AllowRebalance()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Replay quarantined records in place. Refuse rollback of current state: LSN/order
// guards apply exactly as in ordinary consumption. Failure evidence remains retained.
func (p Processor) Replay(ctx context.Context) error {
	rows, err := p.DB.Query(ctx, "SELECT topic,partition_id,offset_id,payload,record_key FROM raw.processing_failures WHERE resolved_at IS NULL ORDER BY topic,partition_id,offset_id")
	if err != nil {
		return err
	}
	type item struct {
		topic      string
		part       int32
		offset     int64
		value, key []byte
	}
	var items []item
	for rows.Next() {
		var i item
		if err = rows.Scan(&i.topic, &i.part, &i.offset, &i.value, &i.key); err != nil {
			rows.Close()
			return err
		}
		items = append(items, i)
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	for _, i := range items {
		e, id, err := cdc.Decode(i.value, i.key, i.topic)
		if err != nil {
			return fmt.Errorf("quarantine still invalid: %w", err)
		}
		tx, err := p.DB.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		state := e.After
		if e.Op == "d" {
			state = e.Before
		}
		order := "0"
		if e.Transaction != nil && e.Transaction.Order != "" {
			order = string(e.Transaction.Order)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO raw.raw_%s(id,data,lsn,source_order,deleted) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET data=excluded.data,lsn=excluded.lsn,source_order=excluded.source_order,deleted=excluded.deleted,updated_at=clock_timestamp() WHERE (excluded.lsn,excluded.source_order)>(raw_%s.lsn,raw_%s.source_order)`, e.Source.Table, e.Source.Table, e.Source.Table), id, state, string(e.Source.LSN), order, e.Op == "d")
		if err == nil {
			_, err = tx.Exec(ctx, "INSERT INTO raw.raw_cdc_events(topic,partition_id,offset_id,source_table,record_id,operation,lsn,envelope) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING", i.topic, i.part, i.offset, e.Source.Table, id, e.Op, string(e.Source.LSN), json.RawMessage(i.value))
		}
		if err == nil {
			_, err = tx.Exec(ctx, "UPDATE raw.processing_failures SET resolved_at=now() WHERE topic=$1 AND partition_id=$2 AND offset_id=$3", i.topic, i.part, i.offset)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
