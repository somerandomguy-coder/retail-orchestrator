with baskets as (
    select * from {{ ref('int_customer_baskets') }}
),

customer_metrics as (
    select
        customer_id,
        count(distinct basket_id) as total_baskets,
        sum(basket_spend) as total_lifetime_spend,
        avg(basket_spend) as avg_basket_value,
        max(basket_at) as last_purchase_at
    from baskets
    group by customer_id
)

select
    customer_id,
    total_baskets,
    total_lifetime_spend,
    round(avg_basket_value, 2) as avg_basket_value,
    last_purchase_at,
    case
        when total_lifetime_spend >= 500 and total_baskets >= 5 then 'Platinum'
        when total_lifetime_spend >= 250 then 'Gold'
        when total_baskets >= 2 then 'Silver'
        else 'Bronze'
    end as loyalty_segment
from customer_metrics
