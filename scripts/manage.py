"""Cross-platform runner. Prerequisites: Python 3.10+ and Docker Compose."""
import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_project_environment():
    """Match Compose's simple .env credentials; shell variables take precedence."""
    path = ROOT / '.env'
    if not path.exists():
        return
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        key, separator, value = line.strip().partition('=')
        if not separator or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key.strip()):
            continue
        value = value.strip()
        quoted = re.fullmatch(r'''(['"])(.*)\1(?:\s+#.*)?''', value)
        value = quoted.group(2) if quoted else value.split(' #', 1)[0].rstrip()
        os.environ.setdefault(key.strip(), value)


load_project_environment()


def generator_report(output):
    """Read the report from stdout, which also contains structured log objects."""
    decoder = json.JSONDecoder()
    cursor = 0
    while (start := output.find('{', cursor)) >= 0:
        try:
            value, length = decoder.raw_decode(output[start:])
        except json.JSONDecodeError:
            cursor = start + 1
            continue
        if isinstance(value, dict) and 'requested_events' in value and 'completed_events' in value:
            return value
        cursor = start + length
    raise ValueError('Generator stdout did not contain a workload report')


def command(args, **kwargs):
    return subprocess.run([str(a) for a in args], cwd=ROOT, check=True, **kwargs)


def compose(*args, **kwargs):
    return command(['docker', 'compose', *args], **kwargs)


def go(*args):
    executable = shutil.which('go')
    if executable and not ('-race' in args and sys.platform == 'win32' and platform.machine().lower() in ('arm64', 'aarch64')):
        return command([executable, *args])
    return command(['docker', 'run', '--rm', '-v', f'{ROOT}:/src', '-v',
                    'biglobal-gomod:/go/pkg/mod', '-v', 'biglobal-gocache:/root/.cache/go-build',
                    '-w', '/src', 'golang:1.26.11-bookworm', 'go', *args])


def python_tool(*args):
    return compose('run', '--rm', '--no-deps', 'reconciliation', *args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action')
    parser.add_argument('--events', type=int)
    args, extra = parser.parse_known_args()
    action = args.action
    if args.events is None:
        args.events = 10000 if action == 'test-integration' else int(os.getenv('EVENTS', '1000'))
    if action == 'build':
        go('build', './...')
        compose('build')
    elif action == 'test':
        go('test', './...')
        python_tool('python', '-m', 'pytest', '-q', '/app/tests')
    elif action == 'test-race':
        go('test', '-race', './...')
    elif action == 'lint':
        go('vet', './...')
        # gofmt -l is checked by CI; go fmt is intentionally explicit here.
        python_tool('ruff', 'check', '--no-cache', '/app/reconciliation', '/app/scripts', '/app/tests')
    elif action == 'up':
        compose('up', '-d', '--build', '--wait', '--wait-timeout', '300')
    elif action == 'down':
        compose('down')
    elif action == 'migrate':
        compose('up', '-d', '--wait', 'source-db', 'analytics-db')
        # initdb applies versioned migrations on an empty volume. Assert the version's objects.
        for service, database, table in [('source-db', 'source', 'trading.executions'),
                                         ('analytics-db', 'warehouse', 'raw.ingestion_checkpoints')]:
            directory = 'source' if service == 'source-db' else 'analytics'
            for migration in sorted((ROOT / 'database' / directory).glob('[0-9]*.sql')):
                if migration.name.startswith('001_'):
                    continue
                compose('exec', '-T', service, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
                        '-d', database, '-f', '/docker-entrypoint-initdb.d/'+migration.name)
            value = compose('exec', '-T', service, 'psql', '-U', 'postgres', '-d', database,
                            '-Atc', f"SELECT to_regclass('{table}')", capture_output=True, text=True).stdout.strip()
            if value != table:
                raise RuntimeError(f'Migration missing: {table}')
    elif action == 'setup-cdc':
        compose('run', '--rm', 'cdc-init')
    elif action == 'verify-cdc':
        command([sys.executable, 'scripts/acceptance.py', '--cdc-only'])
    elif action == 'seed':
        compose('run', '--rm', 'generator', 'load-generator', '-events', '0', '-accounts', '10', *extra)
    elif action in ('generate-events', 'benchmark'):
        if action == 'benchmark':
            command([sys.executable, 'scripts/benchmark.py', '--events', args.events, *extra])
        else:
            compose('run', '--rm', 'generator', 'load-generator', '-events', str(args.events), *extra)
    elif action == 'process-events':
        compose('up', '-d', 'cdc-consumer')
    elif action in ('dbt-build', 'dbt-test'):
        compose('run', '--rm', 'dbt', 'python', '/app/scripts/run_dbt.py',
                'build' if action == 'dbt-build' else 'test', *extra)
    elif action == 'reconcile':
        python_tool('python', '-m', 'reconciliation.engine')
    elif action == 'test-integration':
        command([sys.executable, 'scripts/acceptance.py', '--events', args.events, *extra])
    elif action == 'failure-test':
        command([sys.executable, 'scripts/failure_tests.py', *extra])
    elif action == 'replay':
        compose('exec', '-T', 'cdc-consumer', 'cdc-consumer', 'replay')
    elif action == 'clean':
        # Deliberately retains all database, Kafka, and audit volumes.
        compose('down', '--remove-orphans')
        for relative in ('dbt/financial_analytics/target', 'dbt/financial_analytics/logs', 'bin'):
            target = (ROOT / relative).resolve()
            if not target.is_relative_to(ROOT):
                raise RuntimeError('Cleanup escaped project root')
            if target.exists():
                shutil.rmtree(target)
    else:
        parser.error(f'Unknown action: {action}')


if __name__ == '__main__':
    main()
