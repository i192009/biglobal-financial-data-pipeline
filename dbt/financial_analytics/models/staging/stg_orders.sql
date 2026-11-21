select id,data->>'account_id' as account_id,data->>'instrument_id' as instrument_id,
 data->>'side' as side,(data->>'quantity')::bigint as quantity
from {{ source('raw','raw_orders') }} where not deleted
