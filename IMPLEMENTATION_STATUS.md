# Implementation and validation status

Delivered locally on **21 November 2026**. The platform is implemented, deployed, and verified against real PostgreSQL, Debezium, Redpanda, Go consumers, dbt, and Python reconciliation. All eleven long-running services have working health checks. No remote CI execution is claimed.

## Executed acceptance

| Check | Verified result |
| --- | --- |
| Real initial snapshot | Existing instruments captured with Debezium snapshot operation `r` |
| Real incremental CDC | Insert, update, delete, before/after history, and the exact Kafka null tombstone verified |
| 10,000-event end-to-end acceptance | PASS; 19,970 source HTTP requests, 9,161 new execution records |
| Source write duration in acceptance | 50.819 seconds; another 1.153 seconds to exact warehouse convergence |
| Duplicate execution rejection | PASS; business totals unchanged |
| Injected missing execution | Reconciliation failed with the exact missing UUID; evidence remained after recovery |
| Fresh consumer-group replay | Zero broker lag; reports and financial totals unchanged |
| Go build, vet, race tests | PASS, including real source and analytics PostgreSQL integration tests |
| Vulnerability scan | No vulnerabilities found using Go 1.26.11 |
| Python checks | Three pytest tests passed; Ruff passed |
| dbt build | 46 results: 45 successful/pass, one non-critical price-age warning, zero failures |
| Source freshness | All 14 checks passed |
| Reconciliation | All 33 critical checks passed; quiet-source freshness can warn separately |
| Audit references | Entity UUIDs match operation responses; migration 002 captured historical metadata repairs through CDC |
| Dashboard | Real browser screenshot captured and inspected; actual SQL and Prometheus data |
| Repeated Compose startup | PASS; unchanged connectors are verified without being re-registered |

The delivery snapshot contains **46,119 active executions**, with equal source and reporting counts and matching source/current-state fingerprints. Additional records remain for corrections and reversals. These counts include earlier test runs; they are not a claim that one workload created every row.

## Executed failure scenarios

All eight passed. Original-record replay reads existing Kafka key/value bytes and republishes those exact bytes; no CDC envelopes are fabricated.

1. Source PostgreSQL outage and CDC recovery.
2. Kafka Connect restart and recovery.
3. Redpanda outage and recovery.
4. Consumer SIGKILL crash and recovery.
5. Analytics PostgreSQL outage, with broker commits unchanged while persistence was unavailable.
6. Duplicate original execution CDC record, with unchanged logical state and reports.
7. Out-of-order original price insert replayed after deletion, without resurrecting the row.
8. A late correction to a January reporting period, with corrected cost and trade totals.

The single-worker Connect reassignment delay is 1 second. Broker lag is unavailable during uninitialized offset sentinels, and connector availability also checks the PostgreSQL replication slot. Recovery asserts data convergence, rather than relying only on a running REST endpoint.

## Measured benchmarks

Windows 11 ARM64, Snapdragon X X126100 / Qualcomm Oryon, 8 cores, 16,758,587,392 bytes physical RAM. Docker exposed 8 CPUs and 8,106,520,576 bytes RAM. PostgreSQL 17.12, Go 1.26.11, Debezium 3.7.1.Final, Redpanda 26.2.7, dbt Core 1.11.17 / postgres adapter 1.11.0.

| Workload events | Source duration | HTTP requests | Post-source convergence | CDC arrival p95 | dbt command duration | Execution records in model input |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 9.717 s | 2,002 | 2.363 s | 1.474 s | 42.795 s | 38,756 |
| 10,000 | 98.483 s | 19,948 | 2.374 s | 1.639 s | 38.960 s | 47,911 |
| 100,000 | Not run | - | - | - | - | - |
| 1,000,000 | Not run | - | - | - | - | - |

Benchmarks used eight workers, forty accounts, two instruments, and seed 42 on an existing persistent dataset. Source duration excludes initial funding/seed operations. Convergence is separate; latency uses SQL insertion time minus Debezium source time, and commit can follow insertion. dbt command time includes container/CLI startup and transforms the existing dataset as well as the new workload. CPU/memory reports are samples, not peaks. The shared host was not an isolated laboratory environment.

## Delivered components

- Transactional Go APIs and generator; exact minor-unit arithmetic, idempotency, account locks, unique business references, cash ledger, fees, positions, corrections, reversals, and audit links.
- Independent source/warehouse databases, credentials, versioned SQL, logical WAL, explicit publication/slot, and retention safeguards.
- Debezium/Kafka Connect and persistent Redpanda; schema-bearing JSON, transaction metadata, primary-key partitioning, snapshots, and tombstones.
- Bounded partition workers; atomic history/state/checkpoint batches, ordering fences, retry/backpressure, durable quarantine, and controlled replay.
- Twenty-six dbt models, account snapshot, tests, freshness, indexed cost-basis intermediates, historical corrections, and full-refresh support.
- Auditable Python reconciliation, authenticated analytics APIs, scheduled transformations, metrics, alerts, Grafana, and lineage.
- Compose, Make/PowerShell commands, acceptance/fault/benchmark automation, GitHub Actions configuration, operations documentation, and 28 interview questions with answers.

## Evidence and limits

- [Consolidated results](benchmarks/results.json)
- [Acceptance and financial reports](benchmarks/acceptance.json)
- [Injected discrepancy evidence](benchmarks/acceptance-reconciliation-failure.json)
- [Failure scenarios](benchmarks/failure-tests.json)
- [1,000-event benchmark](benchmarks/1000.json) / [10,000-event benchmark](benchmarks/10000.json)
- [Reconciliation export](benchmarks/reconciliation.json)
- [Real dashboard screenshot](docs/screenshots/financial-operations.png)

The 100,000- and 1,000,000-event sizes are supported CLI targets but unmeasured. The repository is private at [i192009/biglobal-financial-data-pipeline](https://github.com/i192009/biglobal-financial-data-pipeline); current remote workflow status is tracked separately in [GitHub Actions](https://github.com/i192009/biglobal-financial-data-pipeline/actions). The evidence above records local validation and does not claim remote CI success. This is a single-node synthetic demonstration with eventually consistent cross-table reporting, latest-only execution corrections, and the boundaries documented in the README. Known old synthetic prices remain marked stale; they were not replaced to hide quality warnings.
