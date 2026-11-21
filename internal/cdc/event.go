package cdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var Tables = map[string]bool{"customers": true, "accounts": true, "account_balances": true, "instruments": true, "orders": true, "executions": true, "account_transactions": true, "deposits": true, "withdrawals": true, "fees": true, "positions": true, "position_movements": true, "price_snapshots": true, "audit_events": true}

type Event struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Op     string          `json:"op"`
	Source struct {
		Table     string      `json:"table"`
		Schema    string      `json:"schema"`
		LSN       json.Number `json:"lsn"`
		Timestamp int64       `json:"ts_ms"`
	} `json:"source"`
	Transaction *struct {
		ID    string      `json:"id"`
		Order json.Number `json:"total_order"`
	} `json:"transaction"`
	SchemaVersion int `json:"schema_version"`
}

func unwrap(value []byte) ([]byte, error) {
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(value, &wrapper); err != nil {
		return nil, err
	}
	if payload, ok := wrapper["payload"]; ok {
		return payload, nil
	}
	return value, nil
}
func Decode(value, key []byte, topic string) (Event, string, error) {
	var e Event
	payload, err := unwrap(value)
	if err != nil {
		return e, "", err
	}
	if err = json.Unmarshal(payload, &e); err != nil {
		return e, "", err
	}
	if e.SchemaVersion > 1 {
		return e, "", fmt.Errorf("unsupported schema version %d", e.SchemaVersion)
	}
	if !Tables[e.Source.Table] || e.Source.Schema != "trading" || topic != "biglobal.trading."+e.Source.Table {
		return e, "", errors.New("unrecognized source/table/topic")
	}
	if e.Op != "c" && e.Op != "r" && e.Op != "u" && e.Op != "d" {
		return e, "", errors.New("unsupported CDC operation")
	}
	if _, err = strconv.ParseUint(string(e.Source.LSN), 10, 64); err != nil {
		return e, "", errors.New("invalid source LSN")
	}
	if e.Transaction != nil && e.Transaction.Order != "" {
		if _, err = strconv.ParseUint(string(e.Transaction.Order), 10, 64); err != nil {
			return e, "", errors.New("invalid transaction order")
		}
	}
	state := e.After
	if e.Op == "d" {
		state = e.Before
	}
	var row struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(state, &row); err != nil || row.ID == "" {
		return e, "", errors.New("missing row identifier")
	}
	k, err := unwrap(key)
	if err != nil {
		return e, "", errors.New("invalid event key")
	}
	var pk struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(k, &pk); err != nil || pk.ID != row.ID {
		return e, "", errors.New("key/row mismatch")
	}
	if e.Op != "d" {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(e.After, &fields)
		if e.Source.Table == "executions" {
			var side string
			_ = json.Unmarshal(fields["side"], &side)
			if side != "buy" && side != "sell" {
				return e, "", errors.New("invalid execution side")
			}
			for _, f := range []string{"quantity", "price_minor", "notional_minor"} {
				var n json.Number
				if json.Unmarshal(fields[f], &n) != nil {
					return e, "", fmt.Errorf("invalid %s", f)
				}
				v, er := n.Int64()
				if er != nil || v <= 0 {
					return e, "", fmt.Errorf("invalid %s", f)
				}
			}
		}
		if currency, ok := fields["currency"]; ok {
			var c string
			_ = json.Unmarshal(currency, &c)
			if c != "USD" && c != "GBP" && c != "EUR" {
				return e, "", errors.New("unsupported currency")
			}
		}
	}
	return e, row.ID, nil
}
func Tombstone(value []byte) bool {
	return len(bytes.TrimSpace(value)) == 0 || strings.TrimSpace(string(value)) == "null"
}
