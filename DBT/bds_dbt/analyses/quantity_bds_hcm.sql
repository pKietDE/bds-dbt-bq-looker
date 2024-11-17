SELECT distinct bds.city
    , count(*) as quantity_bds_hcm    
FROM {{ ref("dim_bds") }} as bds
where bds.city = "Hồ Chí Minh"
GROUP BY bds.city