"""Health requires a successful, recent real dbt build, not a running process."""
import os

import psycopg

with psycopg.connect(os.environ['ANALYTICS_DSN'], connect_timeout=5) as connection:
    row = connection.execute("""SELECT success AND created_at>now()-interval '5 minutes'
        FROM operations.dbt_runs WHERE results->'args'->>'which'='build'
        ORDER BY created_at DESC LIMIT 1""").fetchone()
raise SystemExit(0 if row and row[0] else 1)
