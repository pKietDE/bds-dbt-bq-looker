SELECT distinct bds.category_home
    , count(*) as quantity_bds    
FROM {{ ref("dim_bds") }} as bds
GROUP BY bds.category_home   
ORDER BY quantity_bds DESC