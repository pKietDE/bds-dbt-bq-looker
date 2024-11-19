

  create or replace view `data-engineering-439905`.`batdongsan`.`quantity_each_category`
  OPTIONS()
  as SELECT distinct bds.category_home
    , count(*) as quantity_bds    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
GROUP BY bds.category_home   
ORDER BY quantity_bds DESC;

