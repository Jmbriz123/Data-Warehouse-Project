-- =====================================================
-- Table: silver.crm_cust_info
-- Purpose: Post-load data quality validation
-- =====================================================


-- ─────────────────────────────────────────────────────
-- 1. RECORD COUNTS  |  total vs distinct vs nulls
-- ─────────────────────────────────────────────────────
-- Expected: total_records = total_unique_records, total_null_records = 0
SELECT
    COUNT(*)                                            AS total_records,
    COUNT(DISTINCT cst_id)                              AS total_unique_records,
    COUNT(*) - COUNT(DISTINCT cst_id)                   AS duplicate_records,
    SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END)    AS total_null_records
FROM silver.crm_cust_info;


-- ─────────────────────────────────────────────────────
-- 2. DUPLICATE / NULL KEY VIOLATIONS
-- ─────────────────────────────────────────────────────
-- Expected: No results
SELECT
    cst_id,
    COUNT(*) AS id_occurrences
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- ─────────────────────────────────────────────────────
-- 3. SURVIVING DUPLICATE ROWS  (version_rank != 1)
-- ─────────────────────────────────────────────────────
-- Expected: No results
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC, _loaded_at DESC   -- tie-breaker
        ) AS version_rank
    FROM silver.crm_cust_info
) ranked
WHERE version_rank != 1;


-- ─────────────────────────────────────────────────────
-- 4. WHITESPACE VIOLATIONS  (leading / trailing spaces)
-- ─────────────────────────────────────────────────────
-- [GENERATOR] Re-run whenever the schema changes; paste output into [CHECK] below.
SELECT
    'SELECT * FROM silver.crm_cust_info WHERE ' ||
    STRING_AGG(
        column_name || ' != TRIM(' || column_name || ')',
        ' OR '
    ) AS check_query
FROM information_schema.columns
WHERE table_schema = 'silver'
  AND table_name   = 'crm_cust_info'
  AND data_type    = 'text';

-- [CHECK]  | Columns: cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr
-- Expected: No results
SELECT * FROM silver.crm_cust_info
WHERE cst_key            != TRIM(cst_key)
   OR cst_firstname      != TRIM(cst_firstname)
   OR cst_lastname       != TRIM(cst_lastname)
   OR cst_marital_status != TRIM(cst_marital_status)
   OR cst_gndr           != TRIM(cst_gndr);


-- ─────────────────────────────────────────────────────
-- 5. LOW-CARDINALITY VALUE AUDITS
-- ─────────────────────────────────────────────────────
SELECT DISTINCT cst_gndr            FROM silver.crm_cust_info ORDER BY 1;
SELECT DISTINCT cst_marital_status  FROM silver.crm_cust_info ORDER BY 1;


-- =====================================================
-- SUMMARY SCOREBOARD
-- Purpose: Single-query pass/fail overview of all checks
-- Expected: issue_count = 0 for every check
-- =====================================================
SELECT check_name, failing_rows,
    CASE WHEN failing_rows = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (

    -- Check 1a: NULL primary keys
    SELECT
        '1a  | NULL cst_id'                             AS check_name,
        SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS failing_rows
    FROM silver.crm_cust_info

    UNION ALL

    -- Check 1b: Duplicate primary keys
    SELECT
        '1b  | Duplicate cst_id'                        AS check_name,
        (CASE WHEN id_with_duplicates IS NULL THEN 0 ELSE 1 END) AS failing_rows
    FROM (
        SELECT COUNT(*) AS id_with_duplicates
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1
    ) AS duplicates

    UNION ALL

    -- Check 3: Rows that should have been deduplicated (version_rank != 1)
    SELECT
        '3   | Surviving duplicate rows'                AS check_name,
        COUNT(*)                                        AS failing_rows
    FROM (
        SELECT ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_date DESC, _loaded_at DESC
               ) AS version_rank
        FROM silver.crm_cust_info
    ) ranked
    WHERE version_rank != 1

    UNION ALL

    -- Check 4: Whitespace violations across all text columns
    SELECT
        '4   | Whitespace violations'                   AS check_name,
        COUNT(*)                                        AS failing_rows
    FROM silver.crm_cust_info
    WHERE cst_key            != TRIM(cst_key)
       OR cst_firstname      != TRIM(cst_firstname)
       OR cst_lastname       != TRIM(cst_lastname)
       OR cst_marital_status != TRIM(cst_marital_status)
       OR cst_gndr           != TRIM(cst_gndr)

    UNION ALL

    -- Check 5a: Unexpected values in cst_gndr
    SELECT
        '5a  | Unexpected cst_gndr values'              AS check_name,
        COUNT(*)                                        AS failing_rows
    FROM silver.crm_cust_info
    WHERE cst_gndr NOT IN ('Male', 'Female', 'n/a')
       OR cst_gndr IS NULL

    UNION ALL

    -- Check 5b: Unexpected values in cst_marital_status
    SELECT
        '5b  | Unexpected cst_marital_status values'    AS check_name,
        COUNT(*)                                        AS failing_rows
    FROM silver.crm_cust_info
    WHERE cst_marital_status NOT IN ('Married', 'Single', 'n/a')
       OR cst_marital_status IS NULL

) checks
ORDER BY check_name;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;