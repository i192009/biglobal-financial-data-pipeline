select id,data->>'account_id' as account_id,data->>'execution_id' as execution_id,
 (data->>'amount_minor')::bigint as amount_minor,data->>'currency' as currency,
 (data->>'reversed')::boolean as reversed
from {{ source('raw','raw_fees') }} where not deleted
