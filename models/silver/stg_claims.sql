WITH cleaned AS (
    SELECT
        CLAIM_ID,
        MEMBER_ID,
        PROVIDER_ID,

        -- Normalize date formats
        TRY_TO_DATE(CLAIM_DATE) AS CLAIM_DATE,

        UPPER(DIAGNOSIS_CODE) AS DIAGNOSIS_CODE,
        PROCEDURE_CODE,

        -- Cast to numeric
        TRY_TO_DECIMAL(CLAIM_AMOUNT, 10, 2) AS CLAIM_AMOUNT,

        UPPER(STATUS) AS STATUS,
        SOURCE_SYSTEM,
        INGESTED_AT
    FROM {{ source('bronze', 'CLAIMS_RAW') }}
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE CLAIM_AMOUNT IS NOT NULL
      AND CLAIM_DATE IS NOT NULL
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY CLAIM_ID
               ORDER BY INGESTED_AT DESC
           ) AS rn
    FROM filtered
)

SELECT
    CLAIM_ID,
    MEMBER_ID,
    PROVIDER_ID,
    CLAIM_DATE,
    DIAGNOSIS_CODE,
    PROCEDURE_CODE,
    CLAIM_AMOUNT,
    STATUS,
    SOURCE_SYSTEM,
    INGESTED_AT
FROM deduped
WHERE rn = 1