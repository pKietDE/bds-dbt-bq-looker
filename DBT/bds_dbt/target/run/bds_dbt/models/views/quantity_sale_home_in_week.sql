

  create or replace view `data-engineering-439905`.`batdongsan`.`quantity_sale_home_in_week`
  OPTIONS()
  as SELECT CAST(COUNT(DISTINCT bds.address) as INT64 ) AS unique_values_in_week
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` AS bds
WHERE DATE_DIFF(CURRENT_DATE(), bds.update_time, DAY) <= 7;

