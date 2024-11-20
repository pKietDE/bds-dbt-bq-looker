

  create or replace view `data-engineering-439905`.`batdongsan`.`avg_area_each_category`
  OPTIONS()
  as SELECT distinct bds.category_home
    , avg(area) as avg_area    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
GROUP BY bds.category_home   
ORDER BY avg_area DESC;
