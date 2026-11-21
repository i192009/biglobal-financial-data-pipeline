"""Measure executed workloads; unexecuted targets never appear as successes."""
import argparse
import json
import platform
import subprocess
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

from acceptance import converged, dbt, http, sql, wait
from manage import ROOT, compose, generator_report


def prometheus(expression):
    try:
        with urlopen('http://localhost:9090/api/v1/query?' + urlencode({'query': expression}), timeout=10) as response:
            result = json.load(response)['data']['result']
        value = result[0]['value'][1] if result else None
        return None if value in ('NaN', '+Inf', '-Inf') else value
    except (OSError, ValueError, KeyError):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--events', type=int, choices=[1000, 10000, 100000, 1000000], default=1000)
    args = parser.parse_args()
    compose('stop', 'transform-scheduler')
    wait(converged, 'benchmark begins with converged state', seconds=600)
    cutoff = sql('analytics-db', 'SELECT clock_timestamp()')
    initial_records = int(sql('analytics-db', 'SELECT count(*) FROM raw.raw_cdc_events'))
    broker_start = prometheus('sum(redpanda_kafka_request_bytes_total)')
    began = time.monotonic()
    try:
        output = compose('run', '--rm', 'generator', 'load-generator', '-events', str(args.events),
                         '-accounts', '40', '-concurrency', '8', '-seed', '42', capture_output=True, text=True).stdout
        # Compose may emit status lines, but generator stdout ends with one JSON object.
        source = generator_report(output)
        assert source['completed_events'] == args.events and source['error'] is None
        source_finished = time.monotonic()
        wait(converged, 'benchmark convergence', seconds=max(300, args.events // 100))
        converged_at = time.monotonic()
        input_executions = int(sql('source-db', 'SELECT count(*) FROM trading.executions'))
        final_records = int(sql('analytics-db', 'SELECT count(*) FROM raw.raw_cdc_events'))
        latency = json.loads(sql('analytics-db', f"""SELECT jsonb_build_object(
            'p50',percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at-source_ts)),
            'p95',percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at-source_ts)),
            'p99',percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at-source_ts)))
            FROM raw.raw_cdc_events WHERE ingested_at>='{cutoff}'::timestamptz AND source_ts IS NOT NULL"""))
        broker_end = prometheus('sum(redpanda_kafka_request_bytes_total)')
        broker_rate = ((float(broker_end)-float(broker_start))/(converged_at-began)
                       if broker_start is not None and broker_end is not None
                       and float(broker_end)>=float(broker_start) else None)
        stats = compose('stats', '--no-stream', '--format', 'json', capture_output=True, text=True).stdout
        started = time.monotonic()
        dbt()
        dbt_seconds = time.monotonic() - started
        query_latencies = []
        for _ in range(20):
            started = time.monotonic()
            http('/analytics/overview', port=8082)
            query_latencies.append((time.monotonic() - started) * 1000)
        hardware = subprocess.run(['docker', 'info', '--format', '{{json .}}'], check=True,
                                  capture_output=True, text=True)
        info = json.loads(hardware.stdout)
        report = {'status': 'PASS', 'created_at': datetime.now(timezone.utc).isoformat(), 'workload': source,
                  'host': platform.platform(), 'docker': {k: info.get(k) for k in ('NCPU', 'MemTotal', 'Architecture', 'OperatingSystem', 'ServerVersion')},
                  'source_phase_seconds_including_startup': source_finished - began,
                  'post_source_convergence_seconds': converged_at - source_finished,
                  'total_convergence_seconds': converged_at - began, 'dbt_build_seconds': dbt_seconds,
                  'analytics_query_latency_ms': {'p50': sorted(query_latencies)[9], 'p95': sorted(query_latencies)[18], 'p99': max(query_latencies)},
                  'replication_latency_seconds': latency,
                  'replication_latency_definition': 'SQL insertion timestamp minus Debezium source timestamp, for this workload only; batch commit may follow insertion.',
                  'new_cdc_history_records': final_records-initial_records,
                  'dbt_input_execution_records': input_executions,
                  'environment_notes': ['The persistent stack includes earlier acceptance/fault fixtures; these are warm shared-host runs.',
                                        'dbt_build_seconds includes container and CLI startup; per-model SQL durations are retained in operations.dbt_runs.'],
                  'consumer_records_per_second': (final_records-initial_records)/(converged_at-began),
                  'broker_bytes_per_second': broker_rate,
                  'container_cpu_memory_sample': stats,
                  'limitations': ['Broker byte counters are sampled by Prometheus and can lag the convergence timestamp; missing samples remain null.',
                                  'CPU/memory are samples taken at convergence, not peak measurements.',
                                  'Initial instrument/account seeding contributes CDC records but is outside the generator HTTP timing.',
                                  'Recovery timings are recorded separately by failure_tests.py.']}
        destination = ROOT / f'benchmarks/{args.events}.json'
        destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report, indent=2))
    finally:
        compose('up', '-d', '--no-deps', 'transform-scheduler')


if __name__ == '__main__':
    main()
