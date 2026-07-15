select
    date(o.ordered_at) as order_date,
    c.country_name,
    c.region,
    count(*) as order_count,
    sum(o.amount) as total_amount
from {{ ref('stg_orders') }} o
left join {{ ref('dim_countries') }} c
    on o.country_code = c.country_code
group by 1, 2, 3
