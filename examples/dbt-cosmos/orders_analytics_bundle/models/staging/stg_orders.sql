with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

deduplicated as (

    select
        order_id,
        customer_id,
        country_code,
        cast(order_ts as timestamp) as ordered_at,
        cast(amount as decimal(12, 2)) as amount,
        row_number() over (
            partition by order_id
            order by order_ts desc
        ) as row_num
    from source

)

select
    order_id,
    customer_id,
    country_code,
    ordered_at,
    amount
from deduplicated
where row_num = 1
