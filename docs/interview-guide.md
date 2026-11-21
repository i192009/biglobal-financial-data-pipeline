# Technical interview guide

1. **What business problem does this solve?** Operational account, execution, cash, fee, and position data must become auditable reports without adding analytical query load to the transactional database. CDC captures committed changes, while independent storage and transformations serve analytics.

2. **How does this differ from traditional batch ETL?** WAL changes flow continuously through an ordered log rather than periodic full-table extraction. dbt remains a scheduled analytical transformation layer; continuous ingestion does not make every reporting model instantaneously current.

3. **Why use Kafka-compatible streaming?** The durable partitioned log decouples source capture from analytical persistence, buffers outages, allows independent consumers, and supports replay. The broker adds operational cost and is justified when those properties matter.

4. **Why use Debezium instead of publishing events in the API?** Debezium reads actual committed PostgreSQL WAL, including database changes outside application code. A naïve database-plus-publish dual write can commit one side while losing the other. This platform does not fabricate CDC envelopes to simulate success.

5. **What does logical replication require?** Logical WAL, a replication-capable login, permissions to read captured tables, a publication, and a persistent replication slot. The connector uses pgoutput and keeps track of its source offsets.

6. **What is the purpose of a publication versus a slot?** The publication selects replicated tables and operations. The slot tracks the consumer's WAL progress and retains needed WAL. They solve separate concerns.

7. **What happens during the initial snapshot?** Debezium reads existing rows and emits snapshot events, then continues streaming changes from its recorded WAL position. The acceptance script checks pre-existing instruments for snapshot operation `r` before testing incremental changes.

8. **How are topics partitioned?** There is one topic per source table and three partitions per topic. Source primary-key JSON is the Kafka key, so updates to one source row retain partition order. Account-wide atomic visibility across separate topics is not guaranteed.

9. **How does Go concurrency preserve order?** A bounded poll produces partition batches. Four fixed workers process separate batches; each batch's records run sequentially. The consumer allows the next rebalance after processing and explicitly committing the fetched batch.

10. **When are Kafka offsets committed?** After the analytics transaction has committed the required history/state/checkpoint or a durable quarantine record. A failed database write is not acknowledged. A failed Kafka commit can safely cause redelivery.

11. **Why also store database checkpoints?** A process can crash after a database commit but before Kafka commit. The durable checkpoint proves that lower/equal offsets already have their effects and prevents repeated application on recovery.

12. **Does this provide exactly-once processing?** It provides at-least-once capture/delivery and idempotent warehouse effects. No atomic distributed transaction spans source PostgreSQL, Connect, Kafka, and analytics PostgreSQL. The description deliberately avoids a broader exactly-once claim.

13. **How are duplicate CDC events recovered?** Identical transport coordinates are deduplicated. Current state also has a source UUID key and an LSN/transaction-order guard. Financial reports use one current logical execution, never the sum of CDC messages. Retained history can contain legitimate repeated deliveries at new transport offsets without inflating business totals.

14. **What about stale or out-of-order data?** Source LSN plus transaction total order fences older state changes. Cross-topic dependencies can be temporarily incomplete; raw tables deliberately have no OLTP foreign keys. dbt/reconciliation run after convergence for a reliable close. Source timeline resets require a fresh namespace.

15. **How are deletes and tombstones different?** A delete envelope includes operation `d` and a before image, and marks the normalized row deleted. A later Kafka null value is a compaction tombstone, not another business deletion or an empty financial record.

16. **What happens to poison records?** They are stored with original bytes, key, topic, partition, offset, error, and timestamp before acknowledgement. This unblocks the partition while surfacing an unresolved failure. Replay revalidates and retains failure history; the system does not silently discard them.

17. **How are financial operations atomic?** An account row lock serializes cash-sensitive operations. Execution, fee, cash transaction, position movement, balance updates, audit evidence, and idempotency response commit together. Constraints and unique execution references add independent protection.

18. **Why avoid floating-point money?** Binary floating point cannot exactly represent most decimal fractions and can accumulate uncontrolled error. Source values are bounded integer minor units; warehouse cost and P&L use fixed-scale decimal NUMERIC. Decimal JSON evidence stays exact.

19. **How is cost basis calculated?** Perpetual weighted average: purchases and their fees increase inventory cost; a sale removes proportional prior cost; net sale proceeds less removed cost is realized P&L. Complete closure zeroes residual cost. SQL unit fixtures cover partial and full closures.

20. **How does dbt handle late corrections?** Reversed flags remain in the UUID-keyed incremental execution fact. Each run rereads all current executions, and dependent financial tables rebuild, restating historical periods from corrected state. No event-time watermark excludes old corrections.

21. **Why separate analytics from transactional operations?** Historical joins, snapshots, recursive cost calculations, and reports can exhaust locks, CPU, cache, and connections needed by source writes. Separate databases and credentials isolate workload and privilege boundaries.

22. **How do you reconcile two databases without a distributed snapshot?** Use repeatable-read snapshots locally, compare counts and currency totals, inspect source audit watermarks, and mark changing/backlogged comparisons INCONCLUSIVE. For a reliable financial close, quiesce source writes and verify convergence. A watermark is not a general distributed transaction barrier.

23. **How do you distinguish missing data from delayed data?** Detect a CDC backlog and source activity before labeling differences final. Investigate durable checkpoints, task health, consumer lag, retained envelopes, and record IDs. The system exposes uncertainty rather than rewriting source or warehouse totals.

24. **How do you recover after WAL retention expires?** Stop and coordinate recovery; the old slot may be invalid. Back up evidence, establish a new source snapshot plus connector topic/slot and warehouse checkpoint namespace, catch up, reconcile, and switch readers. Blindly dropping the slot under a running connector risks data loss.

25. **Why are missing prices NULL?** Invented prices would create fabricated P&L. Price snapshots are explicitly synthetic and timestamped. Reports select an eligible historical snapshot and warn if missing or stale; no FX rate is assumed.

26. **Which benchmarks are credible?** Only executed workloads with recorded event definitions, request counts, source duration, convergence duration, percentiles, environment, and errors. A configured million-event target proves nothing about achieved throughput.

27. **What are the main production gaps?** Single-node durability, shared bearer auth, no TLS/secret management, bounded local WAL retention, no global transaction visibility barrier, expensive full dependent rebuilds, no fractional securities/shorts/FX, and latest-only corrections. These are explicit engineering boundaries, not hidden production guarantees.

28. **How are failures tested?** Executable tests stop actual services, generate source transactions, inspect broker commits during warehouse outages, restart components, wait for source/state fingerprints to converge, rerun dbt, and check reconciliation. The status file reports what was actually run.
