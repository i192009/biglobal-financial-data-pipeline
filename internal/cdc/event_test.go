package cdc

import (
	"encoding/json"
	"strings"
	"testing"
)

const insert = `{"before":null,"after":{"id":"a","quantity":3,"price_minor":1999,"notional_minor":5997,"side":"buy","currency":"USD"},"op":"c","source":{"schema":"trading","table":"executions","lsn":9007199254740993,"ts_ms":1700000000000},"transaction":{"id":"123","total_order":2}}`

func TestDecodePreservesLargeLSN(t *testing.T) {
	e, id, err := Decode([]byte(insert), []byte(`{"id":"a"}`), "biglobal.trading.executions")
	if err != nil || id != "a" || e.Source.LSN.String() != "9007199254740993" || e.Transaction.Order.String() != "2" {
		t.Fatalf("event lost precision: %+v %s %v", e, id, err)
	}
	wrapped, _ := json.Marshal(map[string]any{"schema": map[string]any{"version": 1}, "payload": json.RawMessage(insert)})
	if _, _, err = Decode(wrapped, []byte(`{"schema":{},"payload":{"id":"a"}}`), "biglobal.trading.executions"); err != nil {
		t.Fatal(err)
	}
}
func TestDecodePoison(t *testing.T) {
	for _, value := range []string{`{`, strings.Replace(insert, `"buy"`, `"short"`, 1), strings.Replace(insert, `"quantity":3`, `"quantity":-3`, 1), strings.Replace(insert, `"USD"`, `"XYZ"`, 1), strings.Replace(insert, `"op":"c"`, `"op":"x"`, 1), strings.Replace(insert, `"before":null`, `"schema_version":2,"before":null`, 1)} {
		if _, _, err := Decode([]byte(value), []byte(`{"id":"a"}`), "biglobal.trading.executions"); err == nil {
			t.Errorf("poison accepted: %s", value)
		}
	}
	if _, _, err := Decode([]byte(insert), []byte(`{"id":"b"}`), "biglobal.trading.executions"); err == nil {
		t.Fatal("key mismatch accepted")
	}
	if _, _, err := Decode([]byte(insert), []byte(`{"id":"a"}`), "biglobal.trading.accounts"); err == nil {
		t.Fatal("topic mismatch accepted")
	}
}
func TestDeleteAndTombstone(t *testing.T) {
	deleted := strings.Replace(insert, `"before":null,"after":`, `"after":null,"before":`, 1)
	deleted = strings.Replace(deleted, `"op":"c"`, `"op":"d"`, 1)
	if e, id, err := Decode([]byte(deleted), []byte(`{"id":"a"}`), "biglobal.trading.executions"); err != nil || e.Op != "d" || id != "a" {
		t.Fatalf("delete lost: %v", err)
	}
	for _, value := range [][]byte{nil, {}, []byte("null")} {
		if !Tombstone(value) {
			t.Fatal("tombstone not recognized")
		}
	}
}
