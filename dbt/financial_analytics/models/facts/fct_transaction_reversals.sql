select t.*,o.amount_minor as original_amount_minor
from {{ ref('stg_account_transactions') }} t join {{ ref('stg_account_transactions') }} o on t.reversal_of=o.id
where t.kind='reversal'
