"""Native fallback for SQL/dbt tests only. This does NOT prove CDC.

Copies a consistent source fixture into raw current-state tables, without creating
CDC history or broker messages. Use only isolated test databases, never a running
pipeline. Real CDC acceptance remains scripts/acceptance.py.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

def main():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from reconciliation.engine import TABLES
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-dsn', required=True)
    parser.add_argument('--analytics-dsn', required=True)
    parser.add_argument('--dbt', default='dbt')
    parser.add_argument('--profiles-dir', required=True)
    args = parser.parse_args()
    with psycopg.connect(args.source_dsn) as source, psycopg.connect(args.analytics_dsn) as analytics:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for table in TABLES:
            records = source.execute(f'SELECT id::text,to_jsonb(t) FROM trading.{table} t').fetchall()
            analytics.execute(f'TRUNCATE raw.raw_{table}')
            with analytics.cursor() as cursor:
                cursor.executemany(f'INSERT INTO raw.raw_{table}(id,data,lsn) VALUES(%s,%s,0)',
                                   [(identifier, Jsonb(data)) for identifier, data in records])
    subprocess.run([args.dbt, 'build', '--project-dir', str(root / 'dbt/financial_analytics'),
                    '--profiles-dir', args.profiles_dir], check=True)
    artifact = json.loads((root / 'dbt/financial_analytics/target/run_results.json').read_text())
    assert all(r['status'] in ('success', 'pass', 'warn') for r in artifact['results'])
    print(json.dumps({'validation': 'SQL fixture only; CDC not exercised',
                      'results': len(artifact['results']), 'elapsed_seconds': artifact['elapsed_time']}))


if __name__ == '__main__':
    main()
