with realized as(select account_id,currency,sum(realized_pnl_minor) as realized_pnl_minor from {{ ref('fct_realized_pnl') }} group by 1,2),
unrealized as(select account_id,currency,sum(unrealized_pnl_minor) as unrealized_pnl_minor,count(*) filter(where price_minor is null) as missing_prices from {{ ref('fct_unrealized_pnl') }} group by 1,2)
select a.id as account_id,a.currency,coalesce(r.realized_pnl_minor,0) as realized_pnl_minor,
case when u.missing_prices>0 then null else coalesce(u.unrealized_pnl_minor,0) end as unrealized_pnl_minor,
coalesce(u.missing_prices,0) as missing_prices
from {{ ref('stg_accounts') }} a left join realized r on a.id=r.account_id and a.currency=r.currency
left join unrealized u on a.id=u.account_id and a.currency=u.currency
