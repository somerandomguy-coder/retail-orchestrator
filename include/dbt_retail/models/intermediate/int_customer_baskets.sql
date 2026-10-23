with pos_transactions as (
    select * from {{ ref('stg_pos_transactions') }}
),

marketplace_transactions as (
    select * from {{ ref('stg_marketplace_transactions') }}
),

all_transactions as (
    select * from pos_transactions
    union all
    select * from marketplace_transactions
)

select
    basket_id,
    customer_id,
    min(transaction_at) as basket_at,
    count(distinct product_id) as total_unique_items,
    sum(quantity) as total_units,
    sum(total_amount) as basket_spend,
    count(distinct product_category) as distinct_categories_count
from all_transactions
group by basket_id, customer_id
