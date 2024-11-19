

  create or replace view `data-engineering-439905`.`batdongsan`.`avg_house_area`
  OPTIONS()
  as SELECT distinct bds.city
    , avg(price) as avg_price    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
GROUP BY bds.city;

