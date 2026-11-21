"""Executable assertions against real PostgreSQL -> Debezium -> broker -> Go -> dbt.

No CDC envelope is fabricated. Source mutations go through the API except the
permitted synthetic price INSERT/UPDATE/DELETE probe and explicit fault injection.
"""
import argparse
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from manage import compose, generator_report

ROOT = Path(__file__).resolve().parents[1]
TOKEN = os.environ.get('API_TOKEN', 'local-synthetic-token')


def sql(service, statement):
    database = 'source' if service == 'source-db' else 'warehouse'
    return compose('exec', '-T', service, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
                   '-d', database, '-Atc', statement, capture_output=True, text=True).stdout.strip()


def http(path, payload=None, key=None, expected=200, port=8080):
    headers = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
    if key:
        headers['Idempotency-Key'] = key
    request = Request(f'http://localhost:{port}{path}',
                      data=json.dumps(payload).encode() if payload is not None else None, headers=headers)
    try:
        with urlopen(request, timeout=30) as response:
            assert response.status == expected
            return json.load(response, parse_float=Decimal)
    except HTTPError as error:
        if error.code != expected:
            raise AssertionError(f'{request.method} {request.full_url}: expected {expected}, received {error.code}: {error.read().decode()}') from error
        return json.load(error)


def wait(predicate, description, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (subprocess.CalledProcessError, OSError, ValueError) as error:
            last = str(error)
        time.sleep(2)
    raise AssertionError(f'Timeout: {description}; last error: {last}')


def converged():
    fields = {
        'accounts': ['id', 'customer_id', 'currency', 'status'],
        'account_balances': ['id', 'cash_minor'],
        'orders': ['id', 'account_id', 'instrument_id', 'side', 'quantity'],
        'executions': ['id', 'sequence_id', 'execution_ref', 'order_id', 'account_id',
                       'instrument_id', 'side', 'quantity', 'price_minor', 'notional_minor',
                       'fee_minor', 'currency', 'reversed'],
        'account_transactions': ['id', 'sequence_id', 'account_id', 'amount_minor', 'currency',
                                 'execution_id', 'reversal_of', 'kind'],
        'fees': ['id', 'execution_id', 'account_id', 'amount_minor', 'currency', 'reversed'],
        'positions': ['id', 'account_id', 'instrument_id', 'quantity'],
        'position_movements': ['id', 'account_id', 'instrument_id', 'execution_id', 'quantity', 'is_reversal'],
        'audit_events': ['id', 'action', 'correlation_id', 'entity_id'],
        'price_snapshots': ['id', 'instrument_id', 'price_minor', 'currency'],
    }
    source_queries, analytics_queries = [], []
    for table, columns in fields.items():
        source_values = ','.join(f'{c}::text' for c in columns)
        analytics_values = ','.join(f"data->>'{c}'" for c in columns)
        digest = "coalesce(sum(('x'||substr(md5(jsonb_build_array({})::text),1,15))::bit(60)::bigint::numeric),0)"
        source_queries.append(f"SELECT '{table}',count(*),{digest.format(source_values)} FROM trading.{table}")
        analytics_queries.append(f"SELECT '{table}',count(*),{digest.format(analytics_values)} FROM raw.raw_{table} WHERE NOT deleted")
    source_query = 'SELECT * FROM (' + ' UNION ALL '.join(source_queries) + ') fingerprints ORDER BY 1'
    analytics_query = 'SELECT * FROM (' + ' UNION ALL '.join(analytics_queries) + ') fingerprints ORDER BY 1'
    return sql('source-db', source_query) == sql('analytics-db', analytics_query)


def verify_cdc():
    with urlopen('http://localhost:8083/connectors/biglobal-source/status', timeout=10) as response:
        status = json.load(response)
    assert status['connector']['state'] == 'RUNNING'
    assert status['tasks'] and all(t['state'] == 'RUNNING' for t in status['tasks'])
    wait(lambda: int(sql('analytics-db', "SELECT count(*) FROM raw.raw_cdc_events WHERE source_table='instruments' AND operation='r'")) >= 3,
         'initial snapshot of pre-existing instruments')
    identifier = str(uuid.uuid4())
    instrument = '00000000-0000-4000-8000-000000000001'
    sql('source-db', f"INSERT INTO trading.price_snapshots(id,instrument_id,price_minor,currency,occurred_at) VALUES('{identifier}','{instrument}',10000,'USD',now())")
    wait(lambda: sql('analytics-db', f"SELECT count(*) FROM raw.raw_price_snapshots WHERE id='{identifier}' AND NOT deleted") == '1', 'CDC insert')
    sql('source-db', f"UPDATE trading.price_snapshots SET price_minor=11000 WHERE id='{identifier}'")
    wait(lambda: sql('analytics-db', f"SELECT data->>'price_minor' FROM raw.raw_price_snapshots WHERE id='{identifier}'") == '11000', 'CDC update')
    sql('source-db', f"DELETE FROM trading.price_snapshots WHERE id='{identifier}'")
    wait(lambda: sql('analytics-db', f"SELECT deleted FROM raw.raw_price_snapshots WHERE id='{identifier}'") == 't', 'CDC delete')
    ops = sql('analytics-db', f"SELECT string_agg(DISTINCT operation,',' ORDER BY operation) FROM raw.raw_cdc_events WHERE record_id='{identifier}'")
    assert ops == 'c,d,u', ops
    coordinates = json.loads(sql('analytics-db', f"SELECT jsonb_build_object('partition',partition_id,'first',min(offset_id),'last',max(offset_id)) FROM raw.raw_cdc_events WHERE record_id='{identifier}' GROUP BY partition_id"))
    # Inspect this probe's actual broker records, including its null tombstone.
    records = compose('exec', '-T', 'redpanda', 'rpk', 'topic', 'consume',
                      'biglobal.trading.price_snapshots', '--partitions', str(coordinates['partition']),
                      '--offset', str(coordinates['first']), '--num', str(coordinates['last']-coordinates['first']+2),
                      '--format', '%k\t%v\n', capture_output=True, text=True, timeout=30).stdout
    operations, tombstone = set(), False
    for line in records.splitlines():
        key, value = line.split('\t', 1)
        key = json.loads(key)
        if key.get('payload', key).get('id') != identifier:
            continue
        if not value or value == 'null':
            tombstone = True
        else:
            event = json.loads(value)
            operations.add(event.get('payload', event)['op'])
    assert operations == {'c', 'u', 'd'} and tombstone, (operations, tombstone)
    return {'initial_snapshot': True, 'insert_update_delete': ops, 'broker_envelopes_verified': True,
            'tombstone_verified': True, 'probe_id': identifier}


def dbt():
    compose('run', '--rm', 'dbt', 'python', '/app/scripts/run_dbt.py', 'build')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--events', type=int, default=10000)
    parser.add_argument('--cdc-only', action='store_true')
    parser.add_argument('--skip-up', action='store_true')
    args = parser.parse_args()
    if not args.skip_up:
        compose('up', '-d', '--build', '--wait', '--wait-timeout', '300')
    evidence = {'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'RUNNING', 'cdc': verify_cdc()}
    if args.cdc_only:
        print(json.dumps(evidence, indent=2))
        return
    compose('stop', 'reconciliation', 'transform-scheduler')
    destination = ROOT / 'benchmarks/acceptance.json'
    replay_name = 'biglobal-replay-' + uuid.uuid4().hex[:12]
    replay_started = False
    try:
        print('Generating source workload', flush=True)
        before = int(sql('source-db', 'SELECT count(*) FROM trading.executions'))
        output = compose('run', '--rm', 'generator', 'load-generator', '-events', str(args.events),
                         '-accounts', '20', '-concurrency', '4', '-seed', '42', capture_output=True, text=True).stdout
        workload = generator_report(output)
        assert workload['completed_events'] == args.events and workload['error'] is None
        evidence['workload'] = workload
        after = int(sql('source-db', 'SELECT count(*) FROM trading.executions'))
        assert after > before
        started = time.monotonic()
        wait(converged, 'financial analytics convergence', seconds=600)
        evidence['post_source_convergence_seconds'] = time.monotonic()-started
        print('Source and warehouse converged; building dbt models', flush=True)
        dbt()
        expected = sql('source-db', 'SELECT count(*) FROM trading.executions WHERE NOT reversed')
        actual = sql('analytics-db', 'SELECT coalesce(sum(executions),0) FROM analytics.mart_daily_trading_summary')
        assert expected == actual, (expected, actual)
        for endpoint in ('overview', 'trades', 'accounts', 'positions', 'pnl', 'cashflows',
                         'exposure', 'reconciliation', 'pipeline-health'):
            assert 'data' in http(f'/analytics/{endpoint}?limit=10', port=8082)
        http('/analytics/trades?limit=1001', expected=400, port=8082)
        http('/analytics/trades?from=invalid-date', expected=400, port=8082)
        try:
            urlopen('http://localhost:8082/analytics/overview', timeout=10)
            raise AssertionError('Analytics endpoint accepted an unauthenticated request')
        except HTTPError as error:
            assert error.code == 401
        trade = json.loads(sql('source-db', 'SELECT row_to_json(t) FROM (SELECT * FROM trading.executions ORDER BY sequence_id DESC LIMIT 1)t'))
        duplicate = {key: trade[key] for key in ('order_id', 'execution_ref', 'quantity', 'price_minor', 'fee_minor', 'occurred_at')}
        http('/api/v1/executions', duplicate, str(uuid.uuid4()), expected=409)
        assert int(sql('source-db', 'SELECT count(*) FROM trading.executions')) == after
        print('API and duplicate checks passed; testing reconciliation failure', flush=True)
        compose('run', '--rm', '--no-deps', 'reconciliation', 'python', '-m', 'reconciliation.engine')
        identifier = trade['id']
        sql('analytics-db', f"CREATE TABLE operations.acceptance_backup AS SELECT * FROM raw.raw_executions WHERE id='{identifier}'; DELETE FROM raw.raw_executions WHERE id='{identifier}'")
        try:
            failure = subprocess.run(['docker', 'compose', 'run', '--rm', '--no-deps', 'reconciliation',
                                      'python', '-m', 'reconciliation.engine'], cwd=ROOT, check=False)
            assert failure.returncode != 0, 'Missing execution did not fail quality gate'
            discrepancy = json.loads((ROOT / 'benchmarks/reconciliation.json').read_text())
            missing = next(c for c in discrepancy['checks'] if c['rule_id'] == 'missing_executions')
            assert missing['status'] == 'FAIL' and missing['observed'] == 1 and identifier in missing['related_record_ids']
            (ROOT / 'benchmarks/acceptance-reconciliation-failure.json').write_text(json.dumps(discrepancy, indent=2))
            evidence['discrepancy_run_id'] = discrepancy['run_id']
        finally:
            sql('analytics-db', 'INSERT INTO raw.raw_executions SELECT * FROM operations.acceptance_backup; DROP TABLE operations.acceptance_backup')
        compose('run', '--rm', '--no-deps', 'reconciliation', 'python', '-m', 'reconciliation.engine')
        assert sql('analytics-db', f"SELECT count(*) FROM operations.reconciliation_results WHERE run_id='{evidence['discrepancy_run_id']}' AND rule_id='missing_executions' AND status='FAIL'") == '1'
        totals = sql('analytics-db', 'SELECT coalesce(sum(trade_volume_minor),0) FROM analytics.mart_daily_trading_summary')
        # Replay from a fresh consumer group must leave logical totals unchanged.
        compose('stop', 'cdc-consumer')
        compose('run', '-d', '--name', replay_name, '--no-deps',
                '-e', 'CONSUMER_GROUP='+replay_name, 'cdc-consumer')
        replay_started = True
        def replay_caught_up():
            output = compose('exec', '-T', 'redpanda', 'rpk', 'group', 'describe',
                             replay_name, '--format', 'json',
                             capture_output=True, text=True).stdout
            groups = json.loads(output)
            if isinstance(groups, dict):
                groups = [groups]
            return bool(groups) and all(g.get('total_lag', -1) == 0 and g.get('state') == 'Stable' for g in groups)
        print('Reconciliation checks passed; replaying real broker history', flush=True)
        wait(replay_caught_up, 'fresh-group replay reaches zero broker lag', seconds=600)
        subprocess.run(['docker', 'rm', '-f', replay_name], check=True)
        replay_started = False
        compose('up', '-d', 'cdc-consumer')
        wait(converged, 'consumer restart recovery')
        dbt()
        assert sql('analytics-db', 'SELECT coalesce(sum(trade_volume_minor),0) FROM analytics.mart_daily_trading_summary') == totals
        assert sql('analytics-db', 'SELECT count(*) FROM raw.processing_failures WHERE resolved_at IS NULL') == '0'
        with urlopen('http://localhost:9090/api/v1/query?query=up', timeout=10) as response:
            metrics = json.load(response)
        assert metrics['status'] == 'success' and metrics['data']['result']
        def targets_healthy():
            with urlopen('http://localhost:9090/api/v1/query?query=up', timeout=10) as response:
                result = json.load(response)
            targets = {r['metric']['job']: r['value'][1] for r in result['data']['result']}
            return all(targets.get(job) == '1' for job in ('source-api', 'analytics-api', 'cdc-consumer', 'redpanda'))
        wait(targets_healthy, 'Prometheus confirms recovered targets')
        evidence.update({'requested_workload_events': args.events, 'new_execution_records': after-before,
                         'source_active_executions': int(expected), 'analytics_active_executions': int(actual),
                         'duplicate_prevention': True, 'missing_execution_detected': True,
                         'restart_totals_unchanged': True, 'prometheus_targets_reported': True,
                         'financial_reports': {name: http('/analytics/'+name+'?limit=1000', port=8082)['data']
                                               for name in ('overview', 'pnl', 'exposure')}})
        evidence.update({'status': 'PASS', 'completed_at': datetime.now(timezone.utc).isoformat()})
        destination.write_text(json.dumps(evidence, indent=2, default=str), encoding='utf-8')
        print(json.dumps({key: value for key, value in evidence.items() if key != 'financial_reports'}, indent=2, default=str))
    except BaseException as error:
        evidence.update({'status': 'FAIL', 'error': str(error)})
        destination.write_text(json.dumps(evidence, indent=2, default=str), encoding='utf-8')
        raise
    finally:
        if replay_started:
            subprocess.run(['docker', 'rm', '-f', replay_name], check=False)
        compose('up', '-d', 'cdc-consumer')
        compose('up', '-d', 'reconciliation', 'transform-scheduler')


if __name__ == '__main__':
    main()
