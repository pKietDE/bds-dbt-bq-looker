SELECT distinct bds.city
    , avg(price) as avg_price    
FROM {{ ref("dim_bds") }} as bds
GROUP BY bds.city