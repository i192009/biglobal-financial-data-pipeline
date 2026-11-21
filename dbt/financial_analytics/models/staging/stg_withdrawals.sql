select id,data->>'account_id' as account_id,data->>'transaction_id' as transaction_id,
 (data->>'amount_minor')::bigint as amount_minor,(data->>'occurred_at')::timestamptz as occurred_at
from {{ source('raw','raw_withdrawals') }} where not deleted
