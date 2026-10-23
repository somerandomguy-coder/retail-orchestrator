-- Singular test: Assert that no customer basket has negative or zero spend.
-- Any returning records indicate a data anomaly violation.
select
    basket_id,
    customer_id,
    basket_spend
from {{ ref('int_customer_baskets') }}
where basket_spend <= 0
