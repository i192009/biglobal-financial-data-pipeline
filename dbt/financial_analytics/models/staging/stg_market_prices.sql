select id,data->>'instrument_id' as instrument_id,(data->>'price_minor')::bigint as price_minor,
 data->>'currency' as currency,(data->>'occurred_at')::timestamptz as occurred_at
from {{ source('raw','raw_price_snapshots') }} where not deleted
