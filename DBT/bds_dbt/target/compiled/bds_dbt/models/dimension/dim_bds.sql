

WITH bds_data AS (
    SELECT * 
    FROM `data-engineering-439905`.`batdongsan`.`thong_tin_ban_dat` as bds
    WHERE bds.update_time NOT IN (' ', '')
)

SELECT 
    CASE WHEN TRIM(address_text) = '' THEN 'không tìm thấy' ELSE LOWER(address_text) END as address
    ,CASE WHEN TRIM(district) = '' THEN 'không tìm thấy' ELSE LOWER(district) END as district
    ,CASE WHEN TRIM(city) = '' THEN 'không tìm thấy' ELSE LOWER(city) END as city
    ,
    CASE 
        WHEN REGEXP_REPLACE(price, r',', '.') LIKE '%tỷ%' 
        THEN SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(price, r',', ''), r' tỷ', '') AS DECIMAL) * 1000000000
        WHEN REGEXP_REPLACE(price, r',', '.') LIKE '%triệu%' 
        THEN SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(price, r',', ''), r' triệu', '') AS DECIMAL) * 1000000
        ELSE 0.0
    END
 as price
    ,CASE WHEN TRIM(price_m2) = '' THEN 'giá thỏa thuận' ELSE LOWER(REGEXP_REPLACE(price_m2, r'triệu', 'tr')) END as price_m2
    ,CASE WHEN TRIM(area) = '' THEN 0 ELSE CAST(TRIM(REGEXP_REPLACE(LOWER(area), r'[^0-9]', '')) AS INT64) END as area
    ,CASE WHEN TRIM(area) = '' THEN 'không tìm thấy' ELSE REGEXP_REPLACE(LOWER(area), r'[^a-zA-Z\s]', '') END as area_type
    ,
    CASE
        WHEN REGEXP_CONTAINS(update_time, r'^\d{2}/\d{2}/\d{4}$') THEN PARSE_DATE('%d/%m/%Y', update_time)
        WHEN REGEXP_CONTAINS(update_time, r'^\d{4}-\d{2}-\d{2}$') THEN DATE(update_time)
    END 
 as update_time
    ,LOWER(link) as link
    ,
    CASE 
        WHEN LOWER(category_home) LIKE '%ban nha mat pho%' THEN 'ban nha mat pho'
        WHEN LOWER(category_home) LIKE '%ban can ho chung cu%' THEN 'ban can ho chung cu'
        WHEN LOWER(category_home) LIKE '%ban dat%' THEN 'ban dat'
        WHEN LOWER(category_home) LIKE '%ban shophouse%' THEN 'ban shophouse'
        WHEN LOWER(category_home) LIKE '%ban kho nha xuong%' THEN 'ban kho nha xuong'
        WHEN LOWER(category_home) LIKE '%ban trang trai khu nghi duong' THEN 'ban trang trai khu nghi duong'
        WHEN LOWER(category_home) LIKE '%ban condotel%' THEN 'ban condotel'
        WHEN LOWER(category_home) LIKE '%ban loai bat dong san khac%' THEN 'ban loai bat dong san khac'
        WHEN LOWER(category_home) LIKE '%ban nha biet thu%' THEN 'ban nha biet thu'
        WHEN LOWER(category_home) LIKE '%ban nha rieng%' THEN 'ban nha biet thu'
        ELSE 'khác'
    END
 as category_home
FROM bds_data