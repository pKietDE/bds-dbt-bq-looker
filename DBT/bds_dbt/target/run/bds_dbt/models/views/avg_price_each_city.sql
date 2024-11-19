

  create or replace view `data-engineering-439905`.`batdongsan`.`avg_price_each_city`
  OPTIONS()
  as SELECT *
FROM(
    SELECT distinct bds.city as city
        , avg(price) as avg_price    
    FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
    GROUP BY bds.city 
) as tb1
WHERE city NOT LIKE '%Không tìm thấy%' and  avg_price > 0;

