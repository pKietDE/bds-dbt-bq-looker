with final as (
    SELECT *
        , ROW_NUMBER() OVER(PARTITION BY bds.address ORDER BY bds.update_time DESC ) as row_number
    FROM {{ ref('dim_bds') }} AS bds
)
SELECT count(*) as quantity_home
FROM final
where row_number = 1