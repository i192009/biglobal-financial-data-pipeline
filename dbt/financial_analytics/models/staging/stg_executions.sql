select id,(data->>'sequence_id')::bigint as sequence_id,data->>'execution_ref' as execution_ref,
 data->>'account_id' as account_id,data->>'instrument_id' as instrument_id,data->>'order_id' as order_id,
 data->>'side' as side,(data->>'quantity')::bigint as quantity,
 (data->>'price_minor')::bigint as price_minor,(data->>'notional_minor')::bigint as notional_minor,
 (data->>'fee_minor')::bigint as fee_minor,data->>'currency' as currency,
 (data->>'reversed')::boolean as reversed,(data->>'occurred_at')::timestamptz as occurred_at,
 (data->>'created_at')::timestamptz as created_at,updated_at as ingested_at
from {{ source('raw','raw_executions') }} where not deleted
