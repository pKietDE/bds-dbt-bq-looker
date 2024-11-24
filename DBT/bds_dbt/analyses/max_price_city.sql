with final as (
    SELECT *
        , ROW_NUMBER() OVER(ORDER BY quantity_transaction DESC) as row_number
    FROM(
        SELECT bds.city
        , count(*) AS quantity_transaction 
        FROM {{ ref("dim_bds") }} AS bds
        GROUP BY bds.city
    ) as tb1
)
SELECT city,quantity_transaction
FROM final
where row_number = 1