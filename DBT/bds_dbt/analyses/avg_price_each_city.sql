SELECT *
FROM(
    SELECT distinct bds.city as city
        , avg(price) as avg_price    
    FROM {{ ref("dim_bds") }} as bds
    GROUP BY bds.city 
) as tb1
WHERE city NOT LIKE '%Không tìm thấy%' and  avg_price > 0