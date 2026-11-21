# Operating the synthetic platform

## Start, inspect, stop

Run `python scripts/manage.py up`. Inspect `docker compose ps`, `docker compose logs cdc-consumer`, and `http://localhost:8083/connectors/biglobal-source/status`. RUNNING must apply to both connector and task. `python scripts/manage.py down` and `clean` retain all financial, broker, Grafana, and Prometheus volumes.

Run `python scripts/manage.py migrate` for existing volumes after updating SQL migrations. Post-bootstrap migrations guard their version in `public.platform_migrations`. Source migration 002 repairs early demo audit references from the recorded operation response; it changes only `audit_events.entity_id`, and Debezium retains those metadata corrections. It never changes ledger amounts or reconciles by rewriting financial data. New API operations write the actual entity UUID directly.

For a running source, check the slot with:

```bash
docker compose exec source-db psql -U postgres -d source -c "SELECT slot_name,active,wal_status,safe_wal_size,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_bytes FROM pg_replication_slots"
```

The local broker is a single node with replication factor one. Persistent volumes permit restarts; this setup does not tolerate loss of the host or disk. Export database and broker backups before changing retention or resetting state. dbt marts can be rebuilt from normalized raw state; source and CDC history require backups.

Kafka Connect's `scheduled.rebalance.max.delay.ms` is set to 1,000 ms for this one-worker deployment. The default can defer reassignment for five minutes after broker membership loss; a running REST endpoint alone does not prove the replication task is assigned. Connector availability therefore also checks that PostgreSQL's replication slot is active. Recovery tests require actual source/warehouse convergence. See the [Kafka worker configuration reference](https://kafka.apache.org/41/generated/connect_config.html).

## Consumer/database interruption

The consumer commits offsets only after its required database writes. A crash after the PostgreSQL commit and before Kafka acknowledgement replays records, and checkpoints skip their logical effects. PostgreSQL failures roll back the batch item. Retries back off within a bounded budget, after which the process exits and Compose restarts it. Kafka rebalances remain blocked while a bounded batch persists. On recovery, wait for count/state fingerprints to converge, run dbt, and reconcile.

Do not run a second consumer group against the same current-state schema as an ordinary scale-out mechanism. Scale with the same consumer group. A separate group is useful only for controlled replay/verification; durable checkpoints still prevent repeated effects. A new warehouse must start with empty current-state and checkpoint tables and consume retained history from the beginning.

## Poison messages

Inspect `raw.processing_failures` including topic, partition, offset, key, original bytes, error, and timestamps. A quarantined record is durably acknowledged to unblock its partition; the quality gate fails until investigated. Fix the decoder contract or the explicitly identified invalid source condition and restart the consumer build. Run `python scripts/manage.py replay`. Replay revalidates bytes, applies source ordering guards, records history, and sets `resolved_at`, retaining original failure evidence. It does not edit the original payload or pretend an invalid record succeeded. A still-invalid record causes replay to fail.

## Corrections and reversals

Use authenticated, idempotent application operations. Never update ledger rows to force reconciliation to pass. Corrections and reversals are limited to the latest active execution per account/instrument. They retain original execution records and append offsetting movements. Run dbt build after convergence. Older reporting periods are restated from current corrected state; raw CDC history preserves what changed and when. This is not a full bitemporal accounting restatement engine.

## Slot invalidation and source restore

`max_slot_wal_keep_size=1GB` bounds retained WAL, not elapsed downtime. A busy source can invalidate the slot quickly during an outage. If the WAL required by the slot is gone, stop writes or take a coordinated source snapshot, stop the connector and consumer, back up evidence, and establish a new connector topic prefix/slot plus an empty analytics state/checkpoint namespace. Snapshot and catch up, verify all counts and financial totals, then switch readers. Do not delete a slot or Connect offsets under an active pipeline or reuse old LSN guards after a source timeline reset.

## Retention

There is no automatic financial history deletion. Broker defaults retain local data until configured policy applies; monitor disk usage. A controlled history retention script in `scripts/retention.sql` removes only old raw envelopes after a verified backup/replay horizon, retaining current state, deletion markers, checkpoints, failures, and reconciliation evidence. Review the configured horizon before execution. Current state and checkpoints must survive any history purge. Restoring data removed from raw state requires a coordinated replay rebuild, not merely restarting a checkpointed consumer.

## Fault commands

```bash
python scripts/failure_tests.py --scenario outages
python scripts/failure_tests.py --scenario corrections
python scripts/failure_tests.py --scenario replay
python scripts/acceptance.py --events 10000
python scripts/benchmark.py --events 1000
```

Fault scripts deliberately interrupt this project's services and add synthetic data. Run against an isolated local stack. They restore services in `finally` blocks; assertion failures remain failures. `benchmarks/*.json` are generated only by executed runs.

The replay scenario reads original key/value bytes from a known Kafka partition and offset and republishes those bytes to the same partition. A duplicate execution must leave reports unchanged. Replaying a price insert after its later delete must not resurrect the row. This exercises real Debezium records; it does not manufacture envelopes or substitute for WAL capture. The underlying command is `cdc-consumer replay-record TOPIC PARTITION OFFSET` inside the consumer container.

Broker lag is unavailable while Redpanda returns an uninitialized signed-offset sentinel. The exporter reports NaN and `kafka_consumer_lag_available=0` in that case. The dashboard excludes that sentinel from historical plots instead of presenting it as actual backlog.

## Data and authentication limits

Shared bearer authentication is appropriate for a loopback demonstration, not multi-tenant account authorization. Credentials are service-specific but local plaintext defaults; TLS, external identity, secret rotation, immutable audit storage, HA, network policies, and production capacity planning are outside this demo. Python reconciliation requires independent source read and warehouse write credentials. dbt uses a third login with no transactional source access.
