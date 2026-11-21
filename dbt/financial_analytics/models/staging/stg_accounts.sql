select id, data->>'customer_id' as customer_id, data->>'currency' as currency,
       data->>'status' as status,(data->>'created_at')::timestamptz as created_at
from {{ source('raw','raw_accounts') }} where not deleted
