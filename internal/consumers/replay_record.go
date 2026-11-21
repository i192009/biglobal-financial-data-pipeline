package consumers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/biglobal/financial-data-pipeline/internal/cdc"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReplayRecord republishes the exact key/value bytes of an actual Kafka record.
// It is a deliberate local fault tool, not a replacement for source WAL capture.
func ReplayRecord(ctx context.Context, brokers []string, topic string, partition int32, offset int64) (int64, error) {
	if !cdc.Tables[strings.TrimPrefix(topic, "biglobal.trading.")] || !strings.HasPrefix(topic, "biglobal.trading.") || partition < 0 || offset < 0 {
		return 0, errors.New("invalid source topic or coordinates")
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(offset)}}))
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	fetched := reader.PollRecords(ctx, 1)
	if err = fetched.Err(); err != nil {
		return 0, err
	}
	records := fetched.Records()
	if len(records) != 1 || records[0].Offset != offset {
		return 0, fmt.Errorf("original Kafka record %s/%d/%d is unavailable", topic, partition, offset)
	}
	original := records[0]
	if _, _, err = cdc.Decode(original.Value, original.Key, topic); err != nil {
		return 0, fmt.Errorf("record is not a valid Debezium envelope: %w", err)
	}
	writer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return 0, err
	}
	defer writer.Close()
	replay := &kgo.Record{Topic: topic, Partition: partition, Key: original.Key, Value: original.Value, Headers: original.Headers, Timestamp: original.Timestamp}
	if err = writer.ProduceSync(ctx, replay).FirstErr(); err != nil {
		return 0, err
	}
	return replay.Offset, nil
}
