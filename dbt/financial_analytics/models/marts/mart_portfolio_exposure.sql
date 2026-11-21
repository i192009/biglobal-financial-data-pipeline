select *,price_minor is null as missing_price,
price_timestamp < now()-interval '1 day' as stale_price
from {{ ref('fct_unrealized_pnl') }}
