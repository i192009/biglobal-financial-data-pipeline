"""Run dbt and persist its real artifact even when a quality gate fails."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

project = Path('/app/dbt/financial_analytics')
with psycopg.connect(os.environ['ANALYTICS_DSN'], autocommit=True) as connection:
    # Serialize manual and scheduled builds across containers, including artifacts.
    connection.execute('SELECT pg_advisory_lock(7810321)')
    started = time.time()
    result = subprocess.run(['dbt', *sys.argv[1:], '--project-dir', str(project),
                             '--profiles-dir', str(project)], check=False)
    artifact = project / 'target' / ('sources.json' if sys.argv[1:3] == ['source', 'freshness'] else 'run_results.json')
    if artifact.exists() and artifact.stat().st_mtime >= started:
        connection.execute('INSERT INTO operations.dbt_runs(success,results) VALUES(%s,%s)',
                           (result.returncode == 0, Jsonb(json.loads(artifact.read_text()))))
    else:
        connection.execute('INSERT INTO operations.dbt_runs(success,results) VALUES(%s,%s)',
                           (result.returncode == 0, Jsonb({'command': sys.argv[1:], 'artifact': 'not produced'})))
sys.exit(result.returncode)
