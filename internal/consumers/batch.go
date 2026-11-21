package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/biglobal/financial-data-pipeline/internal/cdc"
	"github.com/biglobal/financial-data-pipeline/internal/observability"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PersistBatch pipelines statements and amortizes WAL fsync over one bounded,
// ordered partition batch. History, state, quarantine and checkpoint stay atomic.
func (p Processor) PersistBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > 500 {
		return errors.New("partition batch exceeds bound")
	}
	first := records[0]
	last := records[len(records)-1]
	for i, r := range records {
		if r.Topic != first.Topic || r.Partition != first.Partition || (i > 0 && r.Offset <= records[i-1].Offset) {
			return errors.New("batch must contain one ordered partition")
		}
	}
	started := time.Now()
	defer func() {
		duration := time.Since(started).Seconds()
		observability.Processing.Observe(duration)
		observability.AnalyticsWrites.Observe(duration)
	}()
	// A committed checkpoint is sufficient proof for a replayed batch. Avoid
	// locking/writing the checkpoint row again when all offsets are durable.
	var durable int64
	err := p.DB.QueryRow(ctx, "SELECT offset_id FROM raw.ingestion_checkpoints WHERE topic=$1 AND partition_id=$2", first.Topic, first.Partition).Scan(&durable)
	if err == nil && durable >= last.Offset {
		return nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "INSERT INTO raw.ingestion_checkpoints(topic,partition_id,offset_id) VALUES($1,$2,-1) ON CONFLICT DO NOTHING", first.Topic, first.Partition)
	if err != nil {
		return err
	}
	var checkpoint int64
	err = tx.QueryRow(ctx, "SELECT offset_id FROM raw.ingestion_checkpoints WHERE topic=$1 AND partition_id=$2 FOR UPDATE", first.Topic, first.Partition).Scan(&checkpoint)
	if err != nil {
		return err
	}
	if checkpoint >= last.Offset {
		return tx.Commit(ctx)
	}
	batch := &pgx.Batch{}
	var timestamps []int64
	rejected, consumed := 0, 0
	for _, r := range records {
		if r.Offset <= checkpoint {
			continue
		}
		consumed++
		if cdc.Tombstone(r.Value) {
			continue
		}
		e, id, decodeErr := cdc.Decode(r.Value, r.Key, r.Topic)
		if decodeErr != nil {
			batch.Queue("INSERT INTO raw.processing_failures(topic,partition_id,offset_id,error,payload,record_key) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", r.Topic, r.Partition, r.Offset, decodeErr.Error(), r.Value, r.Key)
			rejected++
			continue
		}
		timestamps = append(timestamps, e.Source.Timestamp)
		batch.Queue(`INSERT INTO raw.raw_cdc_events(topic,partition_id,offset_id,source_table,record_id,operation,lsn,source_ts,envelope) VALUES($1,$2,$3,$4,$5,$6,$7,to_timestamp($8::double precision/1000),$9) ON CONFLICT DO NOTHING`, r.Topic, r.Partition, r.Offset, e.Source.Table, id, e.Op, string(e.Source.LSN), e.Source.Timestamp, json.RawMessage(r.Value))
		state := e.After
		if e.Op == "d" {
			state = e.Before
		}
		order := "0"
		if e.Transaction != nil && e.Transaction.Order != "" {
			order = string(e.Transaction.Order)
		}
		query := fmt.Sprintf(`INSERT INTO raw.raw_%s(id,data,lsn,source_order,source_ts,deleted) VALUES($1,$2,$3,$4,to_timestamp($5::double precision/1000),$6) ON CONFLICT(id) DO UPDATE SET data=excluded.data,lsn=excluded.lsn,source_order=excluded.source_order,source_ts=excluded.source_ts,deleted=excluded.deleted,updated_at=clock_timestamp() WHERE (excluded.lsn,excluded.source_order)>(raw_%s.lsn,raw_%s.source_order)`, e.Source.Table, e.Source.Table, e.Source.Table)
		batch.Queue(query, id, state, string(e.Source.LSN), order, e.Source.Timestamp, e.Op == "d")
	}
	batch.Queue("UPDATE raw.ingestion_checkpoints SET offset_id=$3,updated_at=clock_timestamp() WHERE topic=$1 AND partition_id=$2", first.Topic, first.Partition, last.Offset)
	results := tx.SendBatch(ctx, batch)
	if err = results.Close(); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	observability.Consumed.Add(float64(consumed))
	observability.Rejected.Add(float64(rejected))
	for _, ts := range timestamps {
		if ts > 0 {
			observability.Latency.Observe(time.Since(time.UnixMilli(ts)).Seconds())
		}
	}
	return nil
}
