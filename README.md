# BI Global Brokers Data Platform

Synthetic financial transactions flow from a transactional Go API through PostgreSQL logical replication, Debezium, Redpanda, and a Go consumer into an independent PostgreSQL warehouse. dbt builds historical reporting models; Python reconciles the two databases; Grafana displays business and pipeline data.

This is a financial data engineering demonstration. It does not connect to brokerage accounts, route orders to exchanges, or execute real trades.

**Repository:** [i192009/biglobal-financial-data-pipeline](https://github.com/i192009/biglobal-financial-data-pipeline) (private). [GitHub Actions](https://github.com/i192009/biglobal-financial-data-pipeline/actions) shows current remote workflow status.

**Delivery evidence:** [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) distinguishes implemented code, checks actually executed, and unmeasured targets. [benchmarks/results.json](benchmarks/results.json) contains measured local validation results; benchmark target sizes are not performance claims. CI is configured in [.github/workflows/ci.yml](.github/workflows/ci.yml); local results do not imply that remote CI has passed.

## Architecture

```mermaid
flowchart LR
  G[Go load generator] --> API[Go financial API]
  API --> S[(Source PostgreSQL)]
  S --> WAL[Logical WAL / pgoutput]
  WAL --> D[Debezium / Kafka Connect]
  D --> K[Redpanda: keyed topics]
  K --> C[Go partition workers]
  C --> A[(Analytics PostgreSQL)]
  A --> DBT[dbt staging / facts / marts]
  DBT --> R[Go analytics API]
  S --> PY[Python reconciliation]
  A --> PY
  A --> GF[Grafana business dashboard]
  C --> P[Prometheus]
  PY --> P
  P --> GF
```

OLTP and warehouse have separate persistent volumes, databases, schemas, users, and connection pools. dbt never queries the transactional source. Go uses HTTP internally where it is sufficient; no unused gRPC scaffolding is included.

## Prerequisites and startup

Install Docker Desktop with a running Linux engine and Docker Compose v2+; Python 3.10+ runs the cross-platform command wrapper. Linux amd64 and arm64 container images are used. Allocate at least 8 GB to Docker for this complete demonstration; concurrent large workloads may need more. Initial image pulls need internet access. Go 1.26 is optional on the host; the wrapper uses a Go build container when Go is absent. Windows ARM hosts should run race tests in the Linux build container: native Windows ARM does not support Go's race detector.

```bash
cp .env.example .env
python scripts/manage.py up
python scripts/manage.py verify-cdc
python scripts/manage.py generate-events --events 1000
python scripts/manage.py dbt-build
python scripts/manage.py reconcile
```

PowerShell:

```powershell
Copy-Item .env.example .env
.\scripts\platform.ps1 up
.\scripts\platform.ps1 verify-cdc
.\scripts\platform.ps1 generate-events --events 1000
.\scripts\platform.ps1 dbt-build
.\scripts\platform.ps1 reconcile
```

Or use the matching `make` targets. The wrapper preserves command exit codes. Fresh PostgreSQL volumes apply the numbered initialization migrations automatically. `make migrate` checks the required migration objects; it does not destructively reinitialize an existing database. Subsequent schema changes require a new explicit migration.

| Service | Local URL | Authentication |
| --- | --- | --- |
| Source API | http://localhost:8080/healthz | Bearer `API_TOKEN` for business routes |
| Analytics API | http://localhost:8082/analytics/overview | Bearer `API_TOKEN` |
| Kafka Connect | http://localhost:8083/connectors/biglobal-source/status | Local loopback development endpoint |
| Prometheus | http://localhost:9090 | Local loopback |
| Grafana | http://localhost:3000/d/biglobal-financial | `admin` / `GRAFANA_PASSWORD` |

Defaults in `.env.example` are local demonstration credentials. The Python wrappers read simple plain or quoted `.env` values, with shell variables taking precedence, so custom API and Grafana credentials also apply to acceptance and screenshot tools. All published ports bind to loopback. Database and broker ports remain on the Compose network. The source and analytics APIs expose `/healthz` and `/metrics`. Container health checks query actual services. Business panels show SQL errors or no-data states until the first successful dbt build. Model rebuilds are scheduled every 60 seconds, and run results are recorded; ingestion runs continuously.

## Commands

Every Make target has `python scripts/manage.py <target>` and `.\scripts\platform.ps1 <target>` equivalents.

| Command | Effect |
| --- | --- |
| `make build` | Compile Go and build service images |
| `make test` | Go unit tests and Python reconciliation tests; database tests skip without explicit test DSNs |
| `make test-race` | Go race detector; run with Linux Go on Windows ARM |
| `make test-integration` | Actual full Compose CDC acceptance with 10,000 workload events |
| `make lint` | Go vet and Python Ruff |
| `make up` / `make down` | Start / stop platform, retain volumes |
| `make migrate` | Initialize fresh databases, apply numbered post-bootstrap migrations, assert objects |
| `make setup-cdc` | Register/update connector and assert connector **and task** readiness |
| `make verify-cdc` | Assert initial snapshot, insert, update, delete, and real broker envelopes |
| `make seed` | Fund ten new synthetic accounts; creates a unique run namespace |
| `make generate-events EVENTS=10000` | Generate real source API mutations |
| `make process-events` | Start the consumer service |
| `make dbt-build` / `make dbt-test` | Transform and run quality gates / rerun tests |
| `make reconcile` | Persist and export discrepancy report; fail on critical FAIL or INCONCLUSIVE |
| `make benchmark EVENTS=1000` | Execute and measure one explicit benchmark target |
| `make failure-test` | Real service outages and late-correction assertions |
| `make clean` | Stop project containers and remove generated build artifacts; retain financial/broker volumes |
| `python scripts/manage.py replay` | Retry validated quarantined records and retain original failure evidence |

To use Linux race tests explicitly:

```bash
docker run --rm -v "${PWD}:/src" -w /src golang:1.26.11-bookworm go test -race ./...
```

## Source API and ledger

All mutations require `Authorization: Bearer <API_TOKEN>`, `Content-Type: application/json`, and `Idempotency-Key`. An exact retry returns the persisted response; a changed payload under the same key conflicts. Keys are scoped across mutation routes. Independent business constraints also make execution references unique.

```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H 'Authorization: Bearer local-synthetic-token' \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-account-1' \
  -d '{"name":"Synthetic analyst account","currency":"USD"}'
```

Required routes: `POST /api/v1/accounts`, `GET /api/v1/accounts/{id}`, `POST /api/v1/orders`, `POST /api/v1/executions`, `POST /api/v1/deposits`, `POST /api/v1/withdrawals`, `GET /api/v1/transactions`, and `GET /api/v1/trades`. Additional routes create/list synthetic instruments, record timestamped prices, and reverse/correct an execution at `POST /api/v1/executions/{id}/reverse` and `/correct`.

Orders specify `account_id`, `instrument_id`, `side`, and integer `quantity`. Executions specify `order_id`, `execution_ref`, `quantity`, `price_minor`, `fee_minor`, and optional RFC3339 `occurred_at`. Cash mutations specify `account_id` and `amount_minor`. Prices specify `instrument_id`, `price_minor`, and `occurred_at`. UUIDs are returned inside `data`.

Each execution locks the account's cash row, checks unfilled order quantity and long holdings, then writes execution, fee, cash ledger, position movement, balances, audit, and idempotency response in one transaction. Deposits and withdrawals use the same account serialization. Reversals append opposite ledger/position movements and flag the original execution and fee. Corrections reverse and replace in a single transaction. Only the latest active execution for an account/instrument may be reversed or corrected; this avoids invalidating subsequent disposals. A buy correction can change the funded amount and must satisfy all ordinary constraints.

Source money uses signed integer minor units, with bounded quantities and prices. USD/GBP/EUR all use two decimal places. Short sales, overdrafts, leverage, fractional shares, FX conversion, corporate actions, settlement, exchange connectivity, and tax reporting are excluded. This is a single-account cash subledger, not a complete double-entry general ledger or a brokerage matching engine.

## Load generator

```bash
docker compose run --rm generator load-generator \
  -events 10000 -eps 500 -accounts 40 -instruments 5 -batch-size 100 \
  -concurrency 8 -seed 42 -start-timestamp 2026-11-20T08:00:00Z \
  -duplicate-rate 0.05 -correction-rate 0.02 -reversal-rate 0.02 -error-rate 0.01
```

An event is a logical workload iteration, which can produce several source commits and several CDC records. Reports distinguish workload events from HTTP requests, execution records, and CDC messages. The deterministic random seed controls workload decisions per worker; run UUIDs and database IDs remain unique. Worker scheduling preserves account affinity. `batch-size` bounds scheduler queues; each financial HTTP operation remains its own transaction. Initial account funding and synthetic instrument/price creation precede the measured source phase.

Targets are 1,000, 10,000, 100,000, and 1,000,000 events. Run each explicitly and inspect generated reports. Measurements include source HTTP percentiles, source request throughput, post-source analytics convergence, SQL arrival-time percentiles for the workload's actual CDC records, dbt elapsed time, analytics query latency, and container resource samples. Broker bytes use Prometheus counters. Missing telemetry is NULL/unavailable. CPU/memory samples are not advertised as peaks.

## CDC, partitioning, and consistency

Source PostgreSQL uses `wal_level=logical`, a dedicated replication login, explicit publication, named persistent slot, and full replica identity for before images. Slot WAL retention is capped at 1 GB to bound disk risk; exceeding it can invalidate the slot and require a coordinated resnapshot. See [docs/operations.md](docs/operations.md).

Kafka Connect runs the Debezium PostgreSQL connector with `pgoutput`, initial snapshot, transaction metadata, schema-bearing JSON, and delete tombstones. Fourteen source tables have separate `biglobal.trading.<table>` topics with three partitions. The primary-key JSON key ensures changes to one source row retain partition affinity. Cross-table transactions can appear at different times; the warehouse is eventually consistent across topics, and reports should be read after convergence for a financial close. No global source transaction barrier is claimed.

The Go consumer polls bounded batches and assigns whole partition batches to four fixed workers. It processes each partition sequentially, blocks rebalances during the batch, retries transient persistence errors, and stops without acknowledging an incomplete batch if retries expire. PostgreSQL transactionally stores the unmodified envelope, normalized current state, and a per-topic/partition offset checkpoint. Only then does the consumer commit Kafka offsets.

Transport duplicates are guarded by `(topic, partition, offset)` and durable checkpoints. Logical state is keyed by source UUID and guarded by `(LSN, transaction total_order)`. Original schemas, operations, before/after, timestamps, and transaction metadata remain in history. Tombstones advance checkpoints after the preceding delete marks current state deleted. Poison messages commit to an auditable quarantine table before acknowledgement and visibly fail quality gates. This is **at-least-once delivery with idempotent effects**, not a claim of end-to-end exactly-once processing. Keep topic partition counts and the source timeline fixed; a source restore/resnapshot needs a coordinated new warehouse/checkpoint namespace.

## dbt and financial reports

Eight staging models feed six intermediate models, six facts, and six marts. The deduplicated execution and cost-basis intermediates are indexed tables, so dependent reports share one cost calculation per build. `fct_trade_executions` uses a UUID incremental unique key and replaces all current source rows on each run, including reversals. Dependent financial tables rebuild in full to propagate late corrections into old periods. This conservative implementation favors correctness over million-row transformation speed. Account attributes have a historical dbt snapshot. Source freshness, relationship tests, financial SQL gates, and dbt unit fixtures are included.

The cost method is perpetual weighted average in source execution sequence. New executions cannot precede an active execution's timestamp for the same account/instrument; latest-only corrections can restate an old reporting period. Buy fees increase cost; sell fees decrease realized proceeds; partial closures remove proportional cost. Full closure sets residual cost to zero. SQL `NUMERIC(38,12)` carries fractional minor units deterministically. Reporting keeps currencies separate and never invents exchange rates. Realized P&L comes from disposals; unrealized P&L uses the latest eligible synthetic instrument price as of the reporting day. Missing prices produce NULL, stale prices produce warnings, and price timestamps are shown. Daily positions carry forward over calendar dates.

Examples covered by dbt's cost-basis test: two buys cost 30,200 minor units for 20 units; selling 5 for 10,000 with a 50 fee realizes 2,400, leaving 22,650 cost; selling the remaining 15 for 30,000 with a 50 fee realizes 7,300 and leaves zero quantity/cost.

## Reconciliation, analytics, and observability

Python persists append-only PASS/FAIL/WARNING/INCONCLUSIVE results with rule ID, severity, expected and observed values, difference, SQL evidence, and related IDs. Checks cover counts, missing/duplicate executions, currency monetary totals, cash, deposits, withdrawals, fees, positions, reversals, orphans, invalid values/timestamps, and sequence balances. Activity crossing independent database snapshots yields INCONCLUSIVE where the source audit watermark proves a backlog or changing source. Reconciliation is intended for a quiescent/converged source; it cannot establish a distributed snapshot during continuous writes. No check rewrites data.

Analytics routes: `/analytics/overview`, `/trades`, `/accounts`, `/positions`, `/pnl`, `/cashflows`, `/exposure`, `/reconciliation`, `/pipeline-health`. Use `limit` (1–1000), `offset`, and `account_id` on account-scoped reports. Trade/cashflow/position/exposure routes support `from` and exclusive `to` RFC3339 timestamps. Responses include `data`, pagination, and units. SQL inputs are parameterized; relation names are fixed allowlists. Queries use a read-only warehouse login. Unsupported filters are documented rather than interpolated into SQL.

Prometheus collects API request counts, source transaction latency, consumed/rejected events, persistence and replication latency, retries, quarantine depth, connector task state, consumer lag, data age, dbt results, and reconciliation failures. Go emits JSON logs with correlation IDs; metric labels avoid account/record IDs. Grafana is provisioned with financial, pipeline, reconciliation history, and lineage panels. Dashboard refresh and dbt refresh are distinct. Real screenshots are recorded only after the running dashboard is inspected; none should be inferred from the JSON provisioning file.

## Tests and recovery

Unit tests cover bounded exact arithmetic, precision-preserving CDC decoding, schema contract validation, deletes/tombstones, and reconciliation classifications. Real PostgreSQL integration tests additionally verify concurrent idempotency, rollback, reversals/corrections, stale-event guards, durable quarantine, and checkpoint behavior. Set `TEST_SOURCE_DSN` / `TEST_ANALYTICS_DSN` against isolated test databases to enable them. Synthetic envelopes in the persistence test are explicitly isolated from the real CDC acceptance test.

`scripts/acceptance.py` asserts snapshot capture, real insert/update/delete envelopes and tombstones in the broker, source/warehouse convergence, dbt marts, API reports, duplicate protection, visible missing-record reconciliation, consumer replay, stable totals, and Prometheus data. `scripts/failure_tests.py` stops/restores actual source, connector, broker, and analytics services, hard-kills the consumer, and records recovery time. It also verifies a historical correction and republishes original broker records to check duplicates and out-of-order arrival. CI runs real dependencies, not mocked CDC.

The dashboard below was captured from the running local services; its values are synthetic financial records, not illustrative chart data.

![Financial operations dashboard](docs/screenshots/financial-operations.png)

See [docs/operations.md](docs/operations.md), [docs/interview-guide.md](docs/interview-guide.md), and [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for recovery steps, design questions, and evidence limitations.

## Technical references

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/connectors/postgresql.html): logical replication, snapshots, transaction metadata, and delivery behavior.
- [Debezium release compatibility](https://debezium.io/releases/): pinned connector release.
- [Redpanda local Docker setup](https://docs.redpanda.com/labs/docker-compose/single-broker/): broker listener and local-mode configuration.
- [dbt PostgreSQL adapter](https://docs.getdbt.com/docs/local/connect-data-platform/postgres-setup): independent analytical target.
- [franz-go consumer configuration](https://github.com/twmb/franz-go/blob/master/pkg/kgo/config.go): explicit commits and blocked rebalances.
