SELECT distinct bds.city
    , count(*) as quantity_bds_hcm    
FROM {{ ref("dim_bds") }} as bds
<<<<<<< HEAD
where bds.city = "Hồ Chí Minh"
=======
where bds.city = "hồ chí minh"
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
GROUP BY bds.city