{% snapshot account_history %}
{{ config(target_schema='analytics',unique_key='id',strategy='check',check_cols=['currency','status'],invalidate_hard_deletes=True) }}
select * from {{ ref('stg_accounts') }}
{% endsnapshot %}
