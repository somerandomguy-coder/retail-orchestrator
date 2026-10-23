with pos as (
    select product_id, product_category, unit_price from {{ ref('stg_pos_transactions') }}
),

marketplace as (
    select product_id, product_category, unit_price from {{ ref('stg_marketplace_transactions') }}
),

combined as (
    select * from pos
    union all
    select * from marketplace
)

select
    product_id,
    product_category,
    round(avg(unit_price), 2) as avg_unit_price,
    round(min(unit_price), 2) as min_unit_price,
    round(max(unit_price), 2) as max_unit_price,
    count(*) as total_occurrences
from combined
group by product_id, product_category
