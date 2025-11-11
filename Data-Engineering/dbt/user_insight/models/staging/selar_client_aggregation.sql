
{{ config(materialized='table',schema='selar_client_timer_insights') }}


WITH databreakDown AS (
        SELECT nameOfClient, 
        brand,time,daytime,unique_row_id ,
        COUNT(daytime) OVER(PARTITION BY brand,daytime
                        ORDER BY brand,daytime) AS daytimeFrequency
        FROM public.selar_oltp_raw_data

)

SELECT * FROM (
    SELECT nameOfClient,brand,time,daytime,daytimeFrequency,
        RANK() OVER(
                    PARTITION BY brand,daytime
                    ORDER BY daytimeFrequency DESC
                    ) 
                    AS timeRank
    FROM databreakDown
)
WHERE timeRank <= 3