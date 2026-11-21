select distinct on(account_id,instrument_id) * from {{ ref('fct_daily_positions') }}
order by account_id,instrument_id,reporting_date desc
