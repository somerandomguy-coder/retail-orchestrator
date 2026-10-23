with source as (
    select * from {{ source('landing', 'pos_transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        basket_id,
        transaction_timestamp::timestamp as transaction_at,
        product_id,
        product_category,
        quantity::int as quantity,
        unit_price::decimal(10,2) as unit_price,
        (quantity * unit_price)::decimal(10,2) as total_amount,
        'POS' as channel
    from source
)

select * from renamed
