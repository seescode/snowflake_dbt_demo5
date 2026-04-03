SELECT
    MEMBER_ID,
    FIRST_NAME,
    LAST_NAME,
    DOB,
    GENDER,
    CITY,
    STATE,
    ZIP,

    -- Derived field
    DATEDIFF(YEAR, DOB, CURRENT_DATE) AS AGE

FROM {{ ref('stg_members') }}