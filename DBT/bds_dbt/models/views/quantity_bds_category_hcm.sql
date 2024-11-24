SELECT distinct {{ convert_category('bds.category_home') }} as category_home
    , count(*) as quantity_bds_hcm    
FROM {{ ref("dim_bds") }} as bds
where bds.city = "hồ chí minh"
<<<<<<< HEAD
GROUP BY {{ convert_category('bds.category_home') }}
=======
GROUP BY {{ convert_category('bds.category_home') }}
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
