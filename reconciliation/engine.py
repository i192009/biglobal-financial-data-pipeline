import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

TABLES = ('customers', 'accounts', 'account_balances', 'instruments', 'orders',
          'executions', 'account_transactions', 'deposits', 'withdrawals',
          'fees', 'positions', 'position_movements', 'price_snapshots', 'audit_events')


def result(rule, expected, observed, *, severity='critical', evidence=None, ids=None):
    difference = None
    if isinstance(expected, (int, Decimal)) and isinstance(observed, (int, Decimal)):
        difference = observed - expected
    elif isinstance(expected, list) and isinstance(observed, list) and all(
            isinstance(row, dict) and 'currency' in row and 'total' in row for row in expected + observed):
        wanted = {row['currency']: row['total'] for row in expected}
        actual = {row['currency']: row['total'] for row in observed}
        difference = {currency: actual.get(currency, 0) - wanted.get(currency, 0)
                      for currency in sorted(wanted.keys() | actual.keys())}
    return {'rule_id': rule, 'severity': severity,
            'status': 'PASS' if expected == observed else ('WARNING' if severity == 'warning' else 'FAIL'),
            'expected': expected, 'observed': observed, 'difference': difference,
            'source_evidence': evidence or {}, 'related_record_ids': (ids or [])[:100]}


def query(connection, sql, args=()):
    return connection.execute(sql, args).fetchall()


def scalar(connection, sql):
    return connection.execute(sql).fetchone()['value']


def serializable(value):
    # Decimal remains a decimal string in JSON; never route money through float.
    return json.loads(json.dumps(value, default=str))


def run(output=None):
    run_id = str(uuid.uuid4())
    checks = []
    with psycopg.connect(os.environ['SOURCE_DSN'], row_factory=dict_row) as source, \
            psycopg.connect(os.environ['ANALYTICS_DSN'], row_factory=dict_row) as warehouse:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        warehouse.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        watermark = scalar(source, 'SELECT count(*) AS value FROM audit_events')
        # Local temporary typed views avoid maintaining a second schema mapping.
        for table in TABLES:
            warehouse.execute(f'CREATE TEMP VIEW check_{table} AS '
                              f'SELECT data, id FROM raw.raw_{table} WHERE NOT deleted')
            expected = scalar(source, f'SELECT count(*) AS value FROM {table}')
            observed = scalar(warehouse, f'SELECT count(*) AS value FROM check_{table}')
            checks.append(result(f'count.{table}', expected, observed,
                                 evidence={'source_table': f'trading.{table}',
                                           'analytics_table': f'raw.raw_{table}'}))
        duplicates = query(warehouse, "SELECT data->>'execution_ref' AS id FROM check_executions GROUP BY 1 HAVING count(*)>1")
        checks.append(result('duplicate_executions', 0, len(duplicates), ids=duplicates))
        source_ids = {r['id'] for r in query(source, 'SELECT id::text AS id FROM executions')}
        analytics_ids = {r['id'] for r in query(warehouse, 'SELECT id FROM check_executions')}
        missing = sorted(source_ids - analytics_ids)
        checks.append(result('missing_executions', 0, len(missing), ids=missing))
        extra = sorted(analytics_ids - source_ids)
        checks.append(result('unexpected_executions', 0, len(extra), ids=extra))
        columns = ('id', 'account_id', 'instrument_id', 'order_id', 'execution_ref',
                   'quantity', 'price_minor', 'notional_minor', 'fee_minor', 'side', 'currency', 'reversed')
        source_fields = ','.join(f'{c}::text' for c in columns)
        analytics_fields = ','.join(f"data->>'{c}'" for c in columns)
        mismatch_count, mismatch_ids = 0, []
        # Streaming merge compares each logical record, so balanced monetary
        # swaps cannot hide behind equal aggregate totals.
        with source.cursor(name='source_execution_records') as left, warehouse.cursor(name='analytics_execution_records') as right:
            left.itersize = right.itersize = 5000
            left.execute(f'SELECT id::text AS id,md5(jsonb_build_array({source_fields})::text) AS fingerprint FROM executions ORDER BY id::text')
            right.execute(f'SELECT id,md5(jsonb_build_array({analytics_fields})::text) AS fingerprint FROM check_executions ORDER BY id')
            a, b = next(left, None), next(right, None)
            while a is not None and b is not None:
                if a['id'] < b['id']:
                    a = next(left, None)
                elif a['id'] > b['id']:
                    b = next(right, None)
                else:
                    if a['fingerprint'] != b['fingerprint']:
                        mismatch_count += 1
                        if len(mismatch_ids) < 100:
                            mismatch_ids.append(a['id'])
                    a, b = next(left, None), next(right, None)
        checks.append(result('execution_record_consistency', 0, mismatch_count, ids=mismatch_ids,
                             evidence={'columns': list(columns), 'method': 'sorted streaming comparison by UUID'}))
        for table, column in [('account_transactions', 'amount_minor'), ('fees', 'amount_minor'),
                              ('executions', 'notional_minor')]:
            expected = query(source, f'SELECT currency,sum({column}) AS total FROM {table} GROUP BY 1 ORDER BY 1')
            observed = query(warehouse, f"SELECT data->>'currency' AS currency,sum((data->>'{column}')::numeric) AS total FROM check_{table} GROUP BY 1 ORDER BY 1")
            checks.append(result(f'monetary_totals.{table}', expected, observed,
                                 evidence={'units': 'minor', 'grouping': 'currency'}))
        violations = {
            'cash_movements': """SELECT b.id FROM check_account_balances b LEFT JOIN (
                SELECT data->>'account_id' AS account_id,sum((data->>'amount_minor')::numeric) AS amount
                FROM check_account_transactions GROUP BY 1) t ON t.account_id=b.id
                WHERE (b.data->>'cash_minor')::numeric<>coalesce(t.amount,0)""",
            'unexpected_negative_balances': "SELECT id FROM check_account_balances WHERE (data->>'cash_minor')::numeric<0",
            'invalid_transaction_sequence': """SELECT id FROM (
                SELECT id,sum((data->>'amount_minor')::numeric) OVER(
                PARTITION BY data->>'account_id' ORDER BY (data->>'sequence_id')::bigint) AS balance
                FROM check_account_transactions) t WHERE balance<0""",
            'position_movements': """SELECT p.id FROM check_positions p LEFT JOIN (
                SELECT data->>'account_id' AS account_id,data->>'instrument_id' AS instrument_id,
                sum((data->>'quantity')::numeric) AS quantity FROM check_position_movements GROUP BY 1,2
                ) m ON m.account_id=p.data->>'account_id' AND m.instrument_id=p.data->>'instrument_id'
                WHERE (p.data->>'quantity')::numeric<>coalesce(m.quantity,0)""",
            'execution_positions': """SELECT p.id FROM check_positions p LEFT JOIN (
                SELECT data->>'account_id' AS account_id,data->>'instrument_id' AS instrument_id,
                sum((data->>'quantity')::numeric*CASE data->>'side' WHEN 'buy' THEN 1 ELSE -1 END) AS quantity
                FROM check_executions WHERE NOT (data->>'reversed')::boolean GROUP BY 1,2
                ) m ON m.account_id=p.data->>'account_id' AND m.instrument_id=p.data->>'instrument_id'
                WHERE (p.data->>'quantity')::numeric<>coalesce(m.quantity,0)""",
            'fee_consistency': """SELECT e.id FROM check_executions e LEFT JOIN check_fees f
                ON f.data->>'execution_id'=e.id WHERE f.id IS NULL
                OR f.data->>'amount_minor'<>e.data->>'fee_minor'
                OR f.data->>'reversed'<>e.data->>'reversed'
                OR f.data->>'currency'<>e.data->>'currency'""",
            'orphaned_records': """SELECT e.id FROM check_executions e LEFT JOIN check_accounts a
                ON a.id=e.data->>'account_id' LEFT JOIN check_orders o ON o.id=e.data->>'order_id'
                LEFT JOIN check_instruments i ON i.id=e.data->>'instrument_id'
                WHERE a.id IS NULL OR o.id IS NULL OR i.id IS NULL""",
            'reversal_handling': """SELECT t.id FROM check_account_transactions t
                LEFT JOIN check_account_transactions o ON o.id=t.data->>'reversal_of'
                WHERE t.data->>'kind'='reversal' AND (o.id IS NULL
                OR (t.data->>'amount_minor')::numeric+(o.data->>'amount_minor')::numeric<>0
                OR t.data->>'account_id'<>o.data->>'account_id')""",
            'invalid_financial_values': """SELECT id FROM check_executions WHERE
                data->>'side' NOT IN ('buy','sell') OR data->>'currency' NOT IN ('USD','GBP','EUR')
                OR (data->>'quantity')::numeric<=0 OR (data->>'price_minor')::numeric<=0
                OR (data->>'notional_minor')::numeric<>(data->>'quantity')::numeric*(data->>'price_minor')::numeric
                OR (data->>'fee_minor')::numeric<0 OR (data->>'occurred_at')::timestamptz<'2000-01-01'
                OR (data->>'occurred_at')::timestamptz>now()+interval '1 day'""",
        }
        for kind in ('deposits', 'withdrawals'):
            sign = 1 if kind == 'deposits' else -1
            violations[f'{kind}_consistency'] = f"""SELECT d.id FROM check_{kind} d
                LEFT JOIN check_account_transactions t ON t.id=d.data->>'transaction_id'
                WHERE t.id IS NULL OR (t.data->>'amount_minor')::numeric<>
                {sign}*(d.data->>'amount_minor')::numeric
                OR t.data->>'account_id'<>d.data->>'account_id'"""
        for rule, sql in violations.items():
            bad = query(warehouse, sql)
            checks.append(result(rule, 0, len(bad), evidence={'sql': sql}, ids=bad))
        failures = scalar(warehouse, 'SELECT count(*) AS value FROM raw.processing_failures WHERE resolved_at IS NULL')
        checks.append(result('unresolved_poison_messages', 0, failures))
        age = scalar(warehouse, 'SELECT extract(epoch FROM now()-max(ingested_at)) AS value FROM raw.raw_cdc_events')
        checks.append(result('analytics_data_freshness', False, age is None or age > 60,
                             severity='warning', evidence={'age_seconds': str(age),
                                                           'quiet_source_can_also_age': True}))
        source.commit()
        current_watermark = scalar(source, 'SELECT count(*) AS value FROM audit_events')
        captured_watermark = scalar(warehouse, 'SELECT count(*) AS value FROM check_audit_events')
        if current_watermark != watermark or captured_watermark < watermark:
            for check in checks:
                if check['status'] == 'FAIL':
                    check['status'] = 'INCONCLUSIVE'
                    check['source_evidence']['reason'] = 'Source activity or CDC backlog crosses the reconciliation snapshot; rerun after convergence.'
        warehouse.commit()
        for check in checks:
            warehouse.execute('''INSERT INTO operations.reconciliation_results
                (run_id,rule_id,status,severity,expected,observed,difference,evidence,related_ids)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (run_id, check['rule_id'], check['status'], check['severity'],
                 Jsonb(serializable(check['expected'])), Jsonb(serializable(check['observed'])),
                 Jsonb(serializable(check['difference'])), Jsonb(check['source_evidence']),
                 Jsonb(serializable(check['related_record_ids']))))
    report = serializable({'run_id': run_id, 'created_at': datetime.now(timezone.utc), 'checks': checks})
    if output:
        Path(output).write_text(json.dumps(report, indent=2), encoding='utf-8')
    return report


def main():
    report = run('/app/benchmarks/reconciliation.json')
    print(json.dumps(report, indent=2))
    if any(c['status'] in ('FAIL', 'INCONCLUSIVE') and c['severity'] == 'critical' for c in report['checks']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
