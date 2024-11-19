SELECT CAST(COUNT(DISTINCT bds.address) as INT64 ) AS unique_values_in_week
FROM {{ ref("dim_bds") }} AS bds
WHERE DATE_DIFF(CURRENT_DATE(), bds.update_time, DAY) <= 7
