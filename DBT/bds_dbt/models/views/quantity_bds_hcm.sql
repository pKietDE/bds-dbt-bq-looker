SELECT distinct bds.city
    , count(*) as quantity_bds_hcm    
FROM {{ ref("dim_bds") }} as bds
where bds.city = "hồ chí minh"
GROUP BY bds.city