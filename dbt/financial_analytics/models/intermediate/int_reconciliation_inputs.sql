select account_id,currency,sum(amount_minor) as ledger_cash_minor
from {{ ref('stg_account_transactions') }} group by account_id,currency
