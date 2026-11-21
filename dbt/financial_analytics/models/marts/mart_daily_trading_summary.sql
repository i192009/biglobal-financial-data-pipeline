select (occurred_at at time zone 'UTC')::date as reporting_date,currency,count(*) as executions,
sum(notional_minor) as trade_volume_minor,sum(fee_minor) as fees_minor
from {{ ref('fct_trade_executions') }} where not reversed group by 1,2
