package consumers

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

// Fabricated envelopes are used only to test persistence invariants. They do not
// establish CDC correctness; scripts/acceptance.py requires real Debezium events.
func TestPostgresPersistenceOrderingAndQuarantine(t *testing.T) {
	dsn := os.Getenv("TEST_ANALYTICS_DSN")
	if dsn == "" {
		t.Skip("TEST_ANALYTICS_DSN required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := Processor{DB: db}
	id := uuid.NewString()
	partition := int32(1000 + time.Now().UnixNano()%1000000)
	topic := "biglobal.trading.accounts"
	defer func() {
		_, _ = db.Exec(ctx, "DELETE FROM raw.raw_accounts WHERE id=$1", id)
		for _, table := range []string{"raw_cdc_events", "ingestion_checkpoints", "processing_failures"} {
			_, _ = db.Exec(ctx, "DELETE FROM raw."+table+" WHERE topic=$1 AND partition_id=$2", topic, partition)
		}
	}()
	record := func(offset, lsn int64, status string) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: partition, Offset: offset, Key: []byte(fmt.Sprintf(`{"id":%q}`, id)), Value: []byte(fmt.Sprintf(`{"before":null,"after":{"id":%q,"currency":"USD","status":%q},"source":{"schema":"trading","table":"accounts","lsn":%d,"ts_ms":1700000000000},"op":"u"}`, id, status, lsn))}
	}
	for _, r := range []*kgo.Record{record(0, 100, "active"), record(0, 100, "active"), record(1, 200, "closed"), record(2, 100, "active"), record(3, 200, "closed")} {
		if err = p.Persist(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	if err = db.QueryRow(ctx, "SELECT data->>'status' FROM raw.raw_accounts WHERE id=$1", id).Scan(&status); err != nil || status != "closed" {
		t.Fatalf("stale state applied: %s %v", status, err)
	}
	var count int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM raw.raw_cdc_events WHERE topic=$1 AND partition_id=$2", topic, partition).Scan(&count); err != nil || count != 4 {
		t.Fatalf("transport dedup history=%d err=%v", count, err)
	}
	poison := record(4, 300, "active")
	poison.Value = []byte(`{"unsupported":"payload"}`)
	if err = p.Persist(ctx, poison); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, "SELECT count(*) FROM raw.processing_failures WHERE topic=$1 AND partition_id=$2", topic, partition).Scan(&count); err != nil || count != 1 {
		t.Fatal("poison was not durably quarantined")
	}
	tombstone := record(5, 300, "closed")
	tombstone.Value = nil
	if err = p.Persist(ctx, tombstone); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = p.Persist(cancelled, record(6, 400, "active")); err == nil {
		t.Fatal("cancelled transaction succeeded")
	}
	var offset int64
	if err = db.QueryRow(ctx, "SELECT offset_id FROM raw.ingestion_checkpoints WHERE topic=$1 AND partition_id=$2", topic, partition).Scan(&offset); err != nil || offset != 5 {
		t.Fatalf("checkpoint advanced without persistence: %d %v", offset, err)
	}
	if err = p.PersistBatch(ctx, []*kgo.Record{record(6, 400, "active"), record(7, 500, "closed")}); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, "SELECT data->>'status' FROM raw.raw_accounts WHERE id=$1", id).Scan(&status); err != nil || status != "closed" {
		t.Fatal("partition batch reordered updates")
	}
	if err = p.PersistBatch(ctx, []*kgo.Record{record(9, 600, "active"), record(8, 700, "closed")}); err == nil {
		t.Fatal("unordered partition batch accepted")
	}
	if err = db.QueryRow(ctx, "SELECT offset_id FROM raw.ingestion_checkpoints WHERE topic=$1 AND partition_id=$2", topic, partition).Scan(&offset); err != nil || offset != 7 {
		t.Fatal("rejected batch advanced checkpoint")
	}
}
