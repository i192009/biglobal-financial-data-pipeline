select (select count(*) from {{ source('raw','processing_failures') }} where resolved_at is null) as unresolved_failures,
(select max(ingested_at) from {{ source('raw','raw_cdc_events') }}) as last_ingested_at,
(select count(*) from {{ source('raw','raw_cdc_events') }}) as captured_events,
(select count(*) from {{ ref('stg_executions') }}) as execution_records
