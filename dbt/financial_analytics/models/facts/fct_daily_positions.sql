with daily as (
 select distinct on(account_id,instrument_id,(occurred_at at time zone 'UTC')::date)
 account_id,instrument_id,currency,(occurred_at at time zone 'UTC')::date as reporting_date,
 holding_quantity,cost_minor
 from {{ ref('int_cost_basis') }} order by account_id,instrument_id,(occurred_at at time zone 'UTC')::date,sequence_id desc
), bounds as (
 select min((occurred_at at time zone 'UTC')::date) as first_day,
 greatest(current_date,max((occurred_at at time zone 'UTC')::date)) as last_day
 from {{ ref('int_cost_basis') }}
), days as (
 select d::date as day from bounds cross join lateral generate_series(first_day,last_day,interval '1 day') d
), pairs as (select distinct account_id,instrument_id,currency from daily)
select p.*,d.day as reporting_date,b.holding_quantity,b.cost_minor,
 m.price_minor,m.occurred_at as price_timestamp,
 b.holding_quantity*m.price_minor::numeric as market_value_minor,
 b.holding_quantity*m.price_minor::numeric-b.cost_minor as unrealized_pnl_minor
from pairs p cross join days d
join lateral(select * from daily x where x.account_id=p.account_id and x.instrument_id=p.instrument_id and x.reporting_date<=d.day order by x.reporting_date desc limit 1)b on true
left join lateral(select * from {{ ref('stg_market_prices') }} m where m.instrument_id=p.instrument_id and m.currency=p.currency and m.occurred_at<((d.day+1)::timestamp at time zone 'UTC') order by m.occurred_at desc,m.id desc limit 1)m on true
