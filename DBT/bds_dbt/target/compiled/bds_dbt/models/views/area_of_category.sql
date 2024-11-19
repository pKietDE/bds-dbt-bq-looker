SELECT distinct bds.category_home
    , bds.area_type
    , sum(area) as sum_area    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
WHERE area_type not like '%không tìm thấy%'
GROUP BY bds.category_home , bds.area_type