select id,(data->>'sequence_id')::bigint as sequence_id,data->>'account_id' as account_id,
 data->>'kind' as kind,(data->>'amount_minor')::bigint as amount_minor,data->>'currency' as currency,
 data->>'execution_id' as execution_id,data->>'reversal_of' as reversal_of,
 (data->>'occurred_at')::timestamptz as occurred_at
from {{ source('raw','raw_account_transactions') }} where not deleted
