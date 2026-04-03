WITH cleaned AS (
    SELECT
        PROVIDER_ID,
        TRIM(PROVIDER_NAME) AS PROVIDER_NAME,
        TRIM(SPECIALTY) AS SPECIALTY,
        NPI,
        TRIM(ADDRESS) AS ADDRESS,
        TRIM(CITY) AS CITY,
        UPPER(STATE) AS STATE,
        ZIP,
        SOURCE_SYSTEM,
        INGESTED_AT
    FROM {{ source('bronze', 'PROVIDERS_RAW') }}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY PROVIDER_ID
               ORDER BY INGESTED_AT DESC
           ) AS rn
    FROM cleaned
)

SELECT *
FROM deduped
WHERE rn = 1