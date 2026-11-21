select id,account_id,instrument_id,currency,occurred_at,sequence_id,realized_pnl_minor
from {{ ref('int_cost_basis') }} where side='sell'
