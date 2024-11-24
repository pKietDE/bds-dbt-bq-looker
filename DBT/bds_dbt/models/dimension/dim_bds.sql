{{ config(
    materialized='table',
    partition_by={
        "field": "update_time",
        "data_type": "DATE"
    },
    cluster_by=["city", "district"]
) }}

WITH bds_data AS (
<<<<<<< HEAD
    SELECT * 
=======
    SELECT distinct * 
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
    FROM {{source('batdongsan_data', 'thong_tin_ban_dat')}} as bds
    WHERE bds.update_time NOT IN (' ', '')
)

SELECT 
    CASE WHEN TRIM(address_text) = '' THEN 'không tìm thấy' ELSE LOWER(address_text) END as address
    ,CASE WHEN TRIM(district) = '' THEN 'không tìm thấy' ELSE LOWER(district) END as district
    ,CASE WHEN TRIM(city) = '' THEN 'không tìm thấy' ELSE LOWER(city) END as city
    ,{{ convert_price('price') }} as price
    ,CASE WHEN TRIM(price_m2) = '' THEN 'giá thỏa thuận' ELSE LOWER(REGEXP_REPLACE(price_m2, r'triệu', 'tr')) END as price_m2
    ,CASE WHEN TRIM(area) = '' THEN 0 ELSE CAST(TRIM(REGEXP_REPLACE(LOWER(area), r'[^0-9]', '')) AS INT64) END as area
    ,CASE WHEN TRIM(area) = '' THEN 'không tìm thấy' ELSE REGEXP_REPLACE(LOWER(area), r'[^a-zA-Z\s]', '') END as area_type
    ,{{ convert_update_time('update_time') }} as update_time
    ,LOWER(link) as link
    ,{{ convert_category('category_home') }} as category_home
FROM bds_data
