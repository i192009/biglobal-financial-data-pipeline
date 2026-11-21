"""Fault scenarios use actual services and assert eventual state convergence."""
import argparse
import json
import subprocess
import time
import uuid

from acceptance import converged, dbt, http, sql, verify_cdc, wait
from manage import ROOT, compose


def generate(events=100):
    return subprocess.Popen(['docker', 'compose', 'run', '--rm', 'generator', 'load-generator',
                             '-events', str(events), '-accounts', '8', '-eps', '25'], cwd=ROOT)


def outages():
    results = []
    for service in ('source-db', 'connect', 'redpanda', 'cdc-consumer', 'analytics-db'):
        print(f'Running outage: {service}', flush=True)
        wait(converged, 'pre-fault convergence')
        compose('stop', 'transform-scheduler', 'reconciliation')
        process = None
        try:
            before = sql('analytics-db', 'SELECT coalesce(sum(offset_id),0) FROM raw.ingestion_checkpoints')
            execution_count = int(sql('source-db', 'SELECT count(*) FROM trading.executions'))
            if service == 'analytics-db':
                compose('stop', service)
                process = generate()
                wait(lambda count=execution_count: int(sql('source-db', 'SELECT count(*) FROM trading.executions')) > count,
                     'source writes continue during analytics outage', seconds=60)
                # Stored checkpoints cannot advance while PostgreSQL is down.
                # Broker commits are checked by inspecting group offsets before/after.
                broker_before = compose('exec', '-T', 'redpanda', 'rpk', 'group', 'describe',
                                        'biglobal-analytics-v1', '--format', 'json', capture_output=True, text=True).stdout
                time.sleep(3)
                broker_after = compose('exec', '-T', 'redpanda', 'rpk', 'group', 'describe',
                                       'biglobal-analytics-v1', '--format', 'json', capture_output=True, text=True).stdout
                def committed(text):
                    groups = json.loads(text)
                    return sorted((p['topic'], p['partition'], p['current_offset'])
                                  for group in groups for p in group['partitions'])
                assert committed(broker_before) == committed(broker_after), 'Offsets advanced during analytics outage'
                compose('up', '-d', '--no-deps', '--wait', service)
            elif service == 'cdc-consumer':
                process = generate()
                wait(lambda count=execution_count: int(sql('source-db', 'SELECT count(*) FROM trading.executions')) > count,
                     'source writes before consumer crash', seconds=60)
                compose('kill', '--signal', 'SIGKILL', service)
                compose('up', '-d', '--no-deps', '--wait', service)
            else:
                compose('stop', service)
                process = generate() if service != 'source-db' else None
                time.sleep(3)
                compose('up', '-d', '--no-deps', '--wait', service)
                if service == 'source-db':
                    process = generate()
            restored = time.monotonic()
            assert process is not None and process.wait(timeout=120) == 0
            # A consumer may have exhausted its retry budget during the outage.
            compose('up', '-d', '--no-deps', 'cdc-consumer')
            wait(converged, f'{service} recovery', seconds=300)
            results.append({'scenario': f'{service}_'+('crash' if service == 'cdc-consumer' else 'outage'), 'status': 'PASS',
                            'checkpoint_offset_sum_before': before,
                            'recovery_seconds': time.monotonic() - restored})
        finally:
            if process is not None and process.poll() is None:
                process.wait(timeout=120)
            compose('up', '-d', '--no-deps', 'source-db', 'analytics-db', 'redpanda', 'connect', 'cdc-consumer')
    return results


def corrections():
    account = http('/api/v1/accounts', {'name': 'Correction fault fixture', 'currency': 'USD'}, str(uuid.uuid4()), 201)['data']['id']
    http('/api/v1/deposits', {'account_id': account, 'amount_minor': 1000000}, str(uuid.uuid4()), 201)
    instrument = '00000000-0000-4000-8000-000000000001'
    order = http('/api/v1/orders', {'account_id': account, 'instrument_id': instrument, 'side': 'buy', 'quantity': 10}, str(uuid.uuid4()), 201)['data']['id']
    trade = http('/api/v1/executions', {'order_id': order, 'execution_ref': str(uuid.uuid4()), 'quantity': 10,
                                      'price_minor': 1000, 'fee_minor': 100, 'occurred_at': '2026-01-01T12:00:00Z'}, str(uuid.uuid4()), 201)['data']['id']
    wait(converged, 'pre-correction convergence')
    dbt()
    http(f'/api/v1/executions/{trade}/correct', {'execution_ref': str(uuid.uuid4()), 'quantity': 10,
                                               'price_minor': 2000, 'fee_minor': 100,
                                               'occurred_at': '2026-01-01T12:00:00Z'}, str(uuid.uuid4()), 201)
    wait(converged, 'late correction')
    dbt()
    assert sql('analytics-db', f"SELECT sum(notional_minor) FROM analytics.fct_trade_executions WHERE account_id='{account}' AND NOT reversed") == '20000'
    assert sql('analytics-db', f"SELECT sum(cost_minor)::bigint FROM analytics.fct_unrealized_pnl WHERE account_id='{account}'") == '20100'
    return {'scenario': 'late_correction_historical_period', 'status': 'PASS'}


def replay_records():
    """Republish original Kafka bytes, never fabricate a Debezium envelope."""
    probe = verify_cdc()['probe_id']
    if sql('source-db', 'SELECT count(*) FROM trading.executions WHERE NOT reversed') == '0':
        assert generate().wait(timeout=120) == 0
    wait(converged, 'pre-replay convergence')
    dbt()
    volume = sql('analytics-db', 'SELECT sum(trade_volume_minor) FROM analytics.mart_daily_trading_summary')
    execution = sql('source-db', 'SELECT id FROM trading.executions WHERE NOT reversed ORDER BY sequence_id DESC LIMIT 1')
    results = []
    for identifier, table, scenario in ((execution, 'executions', 'duplicate_original_cdc_record'),
                                         (probe, 'price_snapshots', 'out_of_order_original_cdc_record')):
        coordinates = json.loads(sql('analytics-db', f"SELECT jsonb_build_object('topic',topic,'partition',partition_id,'offset',offset_id,'lsn',lsn) FROM raw.raw_cdc_events WHERE record_id='{identifier}' AND operation='c' ORDER BY offset_id LIMIT 1"))
        before_state = sql('analytics-db', f"SELECT to_jsonb(r)::text FROM raw.raw_{table} r WHERE id='{identifier}'")
        before_events = int(sql('analytics-db', f"SELECT count(*) FROM raw.raw_cdc_events WHERE record_id='{identifier}'"))
        published = compose('exec', '-T', 'cdc-consumer', 'cdc-consumer', 'replay-record',
                            coordinates['topic'], str(coordinates['partition']), str(coordinates['offset']),
                            capture_output=True, text=True).stdout
        replay = json.loads(published)
        assert replay['appended_offset'] > coordinates['offset']
        wait(lambda record_id=identifier, count=before_events: int(sql('analytics-db', f"SELECT count(*) FROM raw.raw_cdc_events WHERE record_id='{record_id}'")) == count+1,
             'replayed original CDC record persisted')
        assert sql('analytics-db', f"SELECT to_jsonb(r)::text FROM raw.raw_{table} r WHERE id='{identifier}'") == before_state
        if table == 'price_snapshots':
            assert sql('analytics-db', f"SELECT deleted FROM raw.raw_price_snapshots WHERE id='{identifier}'") == 't'
        results.append({'scenario': scenario, 'status': 'PASS', 'record_id': identifier,
                        'original_coordinates': coordinates, 'replayed_offset': replay['appended_offset']})
    dbt()
    assert sql('analytics-db', 'SELECT sum(trade_volume_minor) FROM analytics.mart_daily_trading_summary') == volume
    wait(converged, 'source unchanged after original-event replay')
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', choices=['all', 'outages', 'corrections', 'replay'], default='all')
    args = parser.parse_args()
    results = []
    destination = ROOT / 'benchmarks/failure-tests.json'
    compose('stop', 'transform-scheduler', 'reconciliation')
    try:
        if args.scenario in ('all', 'outages'):
            results += outages()
        if args.scenario in ('all', 'replay'):
            results += replay_records()
        if args.scenario in ('all', 'corrections'):
            results.append(corrections())
    except BaseException as error:
        destination.write_text(json.dumps({'status': 'FAIL', 'error': str(error), 'results': results}, indent=2))
        raise
    finally:
        compose('up', '-d', '--no-deps', 'reconciliation', 'transform-scheduler')
    report = {'status': 'PASS', 'results': results}
    destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
