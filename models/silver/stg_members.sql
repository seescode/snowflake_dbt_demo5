WITH cleaned AS (
    SELECT
        MEMBER_ID,
        TRIM(FIRST_NAME) AS FIRST_NAME,
        TRIM(LAST_NAME) AS LAST_NAME,

        -- Normalize DOB formats
        TRY_TO_DATE(DOB) AS DOB,

        UPPER(GENDER) AS GENDER,
        TRIM(ADDRESS) AS ADDRESS,
        TRIM(CITY) AS CITY,
        UPPER(STATE) AS STATE,
        ZIP,
        SOURCE_SYSTEM,
        INGESTED_AT
    FROM {{ source('bronze', 'MEMBERS_RAW') }}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY MEMBER_ID
               ORDER BY INGESTED_AT DESC
           ) AS rn
    FROM cleaned
)

SELECT
    MEMBER_ID,
    FIRST_NAME,
    LAST_NAME,
    DOB,
    GENDER,
    ADDRESS,
    CITY,
    STATE,
    ZIP,
    SOURCE_SYSTEM,
    INGESTED_AT
FROM deduped
WHERE rn = 1