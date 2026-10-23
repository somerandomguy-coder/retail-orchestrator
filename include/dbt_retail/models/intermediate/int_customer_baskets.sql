with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    basket_id,
    customer_id,
    min(transaction_at) as basket_at,
    count(distinct product_id) as total_unique_items,
    sum(quantity) as total_units,
    sum(total_amount) as basket_spend,
    count(distinct product_category) as distinct_categories_count
from transactions
group by basket_id, customer_id
