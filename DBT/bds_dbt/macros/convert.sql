{% macro convert_price(price_column) %}
    CASE 
        WHEN REGEXP_REPLACE({{ price_column }}, r',', '.') LIKE '%tỷ%' 
        THEN SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE({{ price_column }}, r',', ''), r' tỷ', '') AS DECIMAL) * 1000000000
        WHEN REGEXP_REPLACE({{ price_column }}, r',', '.') LIKE '%triệu%' 
        THEN SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE({{ price_column }}, r',', ''), r' triệu', '') AS DECIMAL) * 1000000
        ELSE 0.0
    END
{% endmacro %}
{% macro convert_category(category_column) %}
    CASE 
        WHEN LOWER({{ category_column }}) LIKE '%ban nha mat pho%' THEN 'ban nha mat pho'
        WHEN LOWER({{ category_column }}) LIKE '%ban can ho chung cu%' THEN 'ban can ho chung cu'
        WHEN LOWER({{ category_column }}) LIKE '%ban dat%' THEN 'ban dat'
        WHEN LOWER({{ category_column }}) LIKE '%ban shophouse%' THEN 'ban shophouse'
        WHEN LOWER({{ category_column }}) LIKE '%ban kho nha xuong%' THEN 'ban kho nha xuong'
        WHEN LOWER({{ category_column }}) LIKE '%ban trang trai khu nghi duong' THEN 'ban trang trai khu nghi duong'
        WHEN LOWER({{ category_column }}) LIKE '%ban condotel%' THEN 'ban condotel'
        WHEN LOWER({{ category_column }}) LIKE '%ban loai bat dong san khac%' THEN 'ban loai bat dong san khac'
        WHEN LOWER({{ category_column }}) LIKE '%ban nha biet thu%' THEN 'ban nha biet thu'
        WHEN LOWER({{ category_column }}) LIKE '%ban nha rieng%' THEN 'ban nha biet thu'
        ELSE 'khác'
    END
{% endmacro %}

{% macro convert_update_time(update_time_column) %}
    CASE
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng hôm nay') THEN CURRENT_DATE()
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng hôm qua') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng 2 ngày trước') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng 3 ngày trước') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng 4 ngày trước') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng 5 ngày trước') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'Đăng 6 ngày trước') THEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'^\d{2}/\d{2}/\d{4}$') THEN PARSE_DATE('%d/%m/%Y', update_time)
        WHEN REGEXP_CONTAINS({{ update_time_column }}, r'^\d{4}-\d{2}-\d{2}$') THEN DATE(update_time)
    END 
{% endmacro %}

