SELECT *
FROM(
    SELECT distinct bds.city as city
        , avg(price) as avg_price    
    FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
    GROUP BY bds.city 
) as tb1
WHERE city NOT LIKE '%Không tìm thấy%' and  avg_price > 0