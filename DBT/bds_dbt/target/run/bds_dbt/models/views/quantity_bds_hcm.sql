

  create or replace view `data-engineering-439905`.`batdongsan`.`quantity_bds_hcm`
  OPTIONS()
  as SELECT distinct bds.city
    , count(*) as quantity_bds_hcm    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
where bds.city = "Hồ Chí Minh"
GROUP BY bds.city;
