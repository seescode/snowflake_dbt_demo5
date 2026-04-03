SELECT
    PROVIDER_ID,
    PROVIDER_NAME,
    SPECIALTY,
    CITY,
    STATE,
    ZIP

FROM {{ ref('stg_providers') }}