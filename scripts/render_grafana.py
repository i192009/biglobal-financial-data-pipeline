"""Render a provisioned dashboard. Every numerical panel queries real services."""
import json
from pathlib import Path

panels = []


def sql(title, query, *, kind='table', width=12, height=7):
    index = len(panels)
    panels.append({'id': index + 1, 'title': title, 'type': kind,
                   'gridPos': {'x': 0 if index % 2 == 0 else 12, 'y': index // 2 * 7, 'w': width, 'h': height},
                   'datasource': {'type': 'postgres', 'uid': 'warehouse'},
                   'targets': [{'refId': 'A', 'rawSql': query, 'format': 'table', 'rawQuery': True}],
                   'fieldConfig': {'defaults': {'noValue': 'Awaiting data / run dbt build'}, 'overrides': []}})


def metric(title, expression, *, kind='timeseries'):
    index = len(panels)
    panels.append({'id': index + 1, 'title': title, 'type': kind,
                   'gridPos': {'x': 0 if index % 2 == 0 else 12, 'y': index // 2 * 7, 'w': 12, 'h': 7},
                   'datasource': {'type': 'prometheus', 'uid': 'prometheus'},
                   'targets': [{'refId': 'A', 'expr': expression, 'legendFormat': title}],
                   'fieldConfig': {'defaults': {'noValue': 'Telemetry unavailable'}, 'overrides': []}})


sql('Executions processed and volume by currency (minor units)', 'SELECT currency,sum(executions) AS executions,sum(trade_volume_minor) AS volume_minor,sum(fees_minor) AS fees_minor FROM analytics.mart_daily_trading_summary GROUP BY 1')
sql('Realized and unrealized P&L by currency (minor units)', 'SELECT currency,sum(realized_pnl_minor) AS realized_pnl_minor,sum(unrealized_pnl_minor) AS unrealized_pnl_minor,sum(missing_prices) AS missing_prices FROM analytics.mart_account_profitability GROUP BY 1')
sql('Account cash balances (minor units)', 'SELECT account_id,currency,ledger_cash_minor FROM analytics.int_reconciliation_inputs ORDER BY 1 LIMIT 100')
sql('Recent executions', 'SELECT occurred_at,execution_ref,account_id,side,quantity,notional_minor,currency,reversed FROM analytics.fct_trade_executions ORDER BY sequence_id DESC LIMIT 30')
metric('CDC records persisted / second', 'rate(cdc_events_consumed_total{job="cdc-consumer"}[1m])')
metric('Kafka committed consumer lag', 'kafka_consumer_lag < 9223372036854775808')
metric('Connector available (1 = running)', 'cdc_connector_available', kind='stat')
metric('Quarantined records', 'cdc_dead_letter_count', kind='stat')
metric('Last ingestion age (seconds; quiet sources also age)', 'analytics_freshness_seconds')
metric('Latest dbt success (1 = pass)', 'dbt_last_run_success', kind='stat')
metric('Replication latency p95 / seconds', 'histogram_quantile(0.95,sum by(le)(rate(cdc_replication_latency_seconds_bucket[5m])))')
metric('Source DB transaction p95 / seconds', 'histogram_quantile(0.95,sum by(le)(rate(source_database_transaction_seconds_bucket[5m])))')
sql('Latest reconciliation status', 'SELECT status,count(*) AS checks FROM operations.reconciliation_results WHERE run_id=(SELECT run_id FROM operations.reconciliation_results ORDER BY created_at DESC LIMIT 1) GROUP BY 1')
sql('Financial discrepancy history (retained across retries)', "SELECT created_at,run_id,rule_id,status,expected::text,observed::text,difference::text,related_ids::text FROM operations.reconciliation_results WHERE status<>'PASS' ORDER BY created_at DESC LIMIT 100")
sql('dbt model results', "SELECT r.created_at,model->>'unique_id' AS model,model->>'status' AS status,model->>'execution_time' AS execution_seconds FROM operations.dbt_runs r CROSS JOIN LATERAL jsonb_array_elements(r.results->'results') model ORDER BY r.created_at DESC LIMIT 100")
sql('Positions and timestamped synthetic prices', 'SELECT account_id,instrument_id,currency,holding_quantity,cost_minor,price_minor,price_timestamp,market_value_minor,missing_price,stale_price FROM analytics.mart_portfolio_exposure ORDER BY 1 LIMIT 100')
index = len(panels)
panels.append({'id': index + 1, 'title': 'Data lineage and interpretation', 'type': 'text',
               'gridPos': {'x': 0, 'y': index // 2 * 7, 'w': 24, 'h': 6},
               'options': {'mode': 'markdown', 'content': '''**Synthetic financial analytics** — refreshes every 10 seconds. Business tables update when dbt runs; ingestion is continuous.

`trading.executions` → PostgreSQL WAL → `biglobal.trading.executions` (3 keyed partitions) → Go validation / durable upsert → `raw.raw_executions` + `raw.raw_cdc_events` → `stg_executions` → `int_cost_basis` → P&L facts → profitability and exposure marts.

`trading.account_transactions` → CDC → `raw.raw_account_transactions` → cash movement models → account balances / cash flows. Python compares independent source and analytics snapshots and retains discrepancy evidence.

Values are in **minor currency units**, currencies stay separate. Missing prices yield NULL P&L. No FX conversion. Initial empty or failed SQL panels require **dbt build**; unavailable metrics remain unavailable.'''}})
dashboard = {'uid': 'biglobal-financial', 'title': 'BI Global Brokers | Financial Data Operations',
             'schemaVersion': 39, 'version': 1, 'refresh': '10s', 'timezone': 'utc',
             'time': {'from': 'now-1h', 'to': 'now'}, 'tags': ['financial', 'cdc', 'synthetic'],
             'panels': panels}
destination = Path(__file__).resolve().parents[1] / 'monitoring/grafana/dashboards/financial.json'
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(json.dumps(dashboard, indent=2), encoding='utf-8')
