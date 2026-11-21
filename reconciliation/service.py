"""Read-only pipeline exporter plus periodic, append-only reconciliation reports."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

import psycopg
from prometheus_client import Gauge, start_http_server
from prometheus_client.parser import text_string_to_metric_families

from reconciliation.engine import run

connector = Gauge('cdc_connector_available', 'Connector and all tasks are RUNNING.')
lag = Gauge('kafka_consumer_lag', 'Broker committed offset lag for analytics consumer group.')
lag_available = Gauge('kafka_consumer_lag_available', 'A finite, initialized broker lag sample is available.')
failures = Gauge('reconciliation_failures', 'Critical failed checks in the latest run.')
freshness = Gauge('analytics_freshness_seconds', 'Seconds since the last durably ingested event.')
deadletters = Gauge('cdc_dead_letter_count', 'Unresolved durable poison messages.')
dbt_success = Gauge('dbt_last_run_success', 'Latest real dbt command succeeded.')
dbt_duration = Gauge('dbt_execution_seconds', 'Elapsed time reported by the latest dbt artifact.')
events = Gauge('cdc_events_persisted', 'Persisted CDC history records.')
ready = False


def collect():
    global ready
    while True:
        try:
            with urlopen('http://connect:8083/connectors/biglobal-source/status', timeout=5) as response:
                status = json.load(response)
            with psycopg.connect(os.environ['SOURCE_DSN'], connect_timeout=5) as source:
                slot_active = source.execute("SELECT coalesce(bool_or(active),false) FROM pg_replication_slots WHERE slot_name='biglobal_slot'").fetchone()[0]
            connector.set(int(status['connector']['state'] == 'RUNNING' and bool(status['tasks'])
                              and all(t['state'] == 'RUNNING' for t in status['tasks']) and slot_active))
        except Exception:
            connector.set(0)
        try:
            with urlopen('http://redpanda:9644/public_metrics', timeout=5) as response:
                metrics = response.read().decode()
            samples = [sample for family in text_string_to_metric_families(metrics)
                       for sample in family.samples
                       if sample.name == 'redpanda_kafka_consumer_group_lag_sum'
                       and sample.labels.get('redpanda_group') == 'biglobal-analytics-v1']
            # Redpanda can expose a signed-offset sentinel during group startup.
            # Preserve it as unavailable rather than plotting it as real backlog.
            available = bool(samples) and all(0 <= sample.value < 2**63 for sample in samples)
            lag_available.set(int(available))
            lag.set(sum(sample.value for sample in samples) if available else float('nan'))
        except Exception:
            lag.set(float('nan'))
            lag_available.set(0)
        try:
            with psycopg.connect(os.environ['ANALYTICS_DSN'], connect_timeout=5) as connection:
                age, count = connection.execute('SELECT extract(epoch FROM now()-max(ingested_at)),count(*) FROM raw.raw_cdc_events').fetchone()
                freshness.set(float(age) if age is not None else float('nan'))
                events.set(count)
                deadletters.set(connection.execute('SELECT count(*) FROM raw.processing_failures WHERE resolved_at IS NULL').fetchone()[0])
                last = connection.execute('SELECT success,results FROM operations.dbt_runs ORDER BY created_at DESC LIMIT 1').fetchone()
                dbt_success.set(int(last[0]) if last else float('nan'))
                dbt_duration.set(last[1].get('elapsed_time', float('nan')) if last else float('nan'))
            ready = True
        except Exception as exc:
            ready = False
            print(json.dumps({'event': 'exporter_error', 'error': str(exc)}), flush=True)
        time.sleep(10)


def reconcile():
    while True:
        try:
            report = run()
            failures.set(sum(c['status'] == 'FAIL' and c['severity'] == 'critical' for c in report['checks']))
            print(json.dumps({'event': 'reconciliation_run', 'run_id': report['run_id']}), flush=True)
        except Exception as exc:
            print(json.dumps({'event': 'reconciliation_error', 'error': str(exc)}), flush=True)
        time.sleep(60)


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if ready else 503)
        self.end_headers()
        self.wfile.write(b'ok' if ready else b'database unavailable')


if __name__ == '__main__':
    start_http_server(8091)
    threading.Thread(target=collect, daemon=True).start()
    threading.Thread(target=reconcile, daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', 8090), Health).serve_forever()
