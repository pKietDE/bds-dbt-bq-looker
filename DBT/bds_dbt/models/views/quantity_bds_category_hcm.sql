SELECT distinct {{ convert_category('bds.category_home') }} as category_home
    , count(*) as quantity_bds_hcm    
FROM {{ ref("dim_bds") }} as bds
where bds.city = "hồ chí minh"
GROUP BY {{ convert_category('bds.category_home') }}
