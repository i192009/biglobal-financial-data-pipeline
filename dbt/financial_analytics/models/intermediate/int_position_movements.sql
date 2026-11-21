select id,account_id,instrument_id,currency,sequence_id,occurred_at,
 case side when 'buy' then quantity else -quantity end as quantity_movement
from {{ ref('int_deduplicated_executions') }}
