{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert', on_schema_change='fail') }}
-- Read all current rows each run, including reversed flags. Replacing by ID means
-- old reporting periods are corrected without a lossy event-time watermark.
select * from {{ ref('stg_executions') }}
