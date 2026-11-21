"""Idempotent connector registration with bounded readiness retries."""
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

base = os.environ.get('CONNECT_URL', 'http://connect:8083')
spec = json.loads(Path('/app/debezium/connectors/source.json').read_text())
deadline = time.monotonic() + 240


def api(url, method='GET', payload=None, missing_ok=False):
    last_error = None
    for attempt in range(5):
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            break
        request = Request(url, method=method,
                          data=json.dumps(payload).encode() if payload is not None else None,
                          headers={'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=min(10, remaining)) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code == 404 and missing_ok:
                return None
            if error.code not in (409, 500, 502, 503, 504):
                raise
            last_error = str(error)
        except (URLError, TimeoutError, ConnectionError) as error:
            last_error = str(error)
        time.sleep(min(attempt+1, 3))
    raise RuntimeError(f'CDC initialization request failed: {url}: {last_error}')


api('http://redpanda:9644/v1/cluster_config', 'PUT',
    {'upsert': {'enable_consumer_group_metrics': ['group', 'partition', 'consumer_lag'],
                'consumer_group_lag_collection_interval_sec': 10}, 'remove': []})
config_url = f'{base}/connectors/{spec["name"]}/config'
existing = api(config_url, missing_ok=True)
changed = existing is None or any(str(existing.get(key)) != str(value) for key, value in spec['config'].items())
# Repeated Compose startup must not restart an unchanged replication task.
if changed:
    api(config_url, 'PUT', spec['config'])
status = {}
while time.monotonic() < deadline:
    status = api(f'{base}/connectors/{spec["name"]}/status', missing_ok=True) or {}
    tasks = status.get('tasks', [])
    if status.get('connector', {}).get('state') == 'RUNNING' and tasks and all(
            task['state'] == 'RUNNING' for task in tasks):
        print(json.dumps({'configuration_changed': changed, **status}, indent=2))
        break
    if any(task['state'] == 'FAILED' for task in tasks):
        raise RuntimeError(json.dumps(status))
    time.sleep(2)
else:
    raise RuntimeError(f'Connector did not become ready: {status}')
