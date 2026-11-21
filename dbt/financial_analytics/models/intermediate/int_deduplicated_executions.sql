{{ config(materialized='table', indexes=[{'columns': ['id'], 'unique': true}, {'columns': ['account_id', 'instrument_id', 'sequence_id']}]) }}
-- One current source row per UUID; never aggregate the CDC history as trade volume.
select * from {{ ref('stg_executions') }} where not reversed
