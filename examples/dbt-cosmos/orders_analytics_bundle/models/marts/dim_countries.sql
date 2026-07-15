select
    country_code,
    country_name,
    region
from {{ ref('country_codes') }}
