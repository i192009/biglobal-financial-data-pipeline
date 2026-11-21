select *,sum(amount_minor) over(partition by account_id,currency order by sequence_id rows unbounded preceding) as cash_balance_minor
from {{ ref('stg_account_transactions') }}
