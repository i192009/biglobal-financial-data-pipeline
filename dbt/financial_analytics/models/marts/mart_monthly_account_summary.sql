select account_id,currency,date_trunc('month',occurred_at at time zone 'UTC')::date as reporting_date,
sum(amount_minor) as net_cash_flow_minor,
sum(case when kind='deposit' then amount_minor else 0 end) as deposits_minor,
-sum(case when kind='withdrawal' then amount_minor else 0 end) as withdrawals_minor
from {{ ref('fct_account_cash_flows') }} group by 1,2,3
