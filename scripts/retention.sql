-- Execute deliberately as ingest_app after backing up history and verifying the
-- replay horizon. Adjust 90 days to an approved local policy. No automatic job.
BEGIN;
SET LOCAL statement_timeout='30s';
DELETE FROM raw.raw_cdc_events e
USING raw.ingestion_checkpoints c
WHERE e.topic=c.topic AND e.partition_id=c.partition_id
AND e.offset_id<=c.offset_id
AND e.ingested_at<now()-interval '90 days';
COMMIT;
