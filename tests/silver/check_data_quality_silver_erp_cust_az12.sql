-- =============================================================================
-- SILVER LAYER: Quality Gate Tests — erp_cust_az12
-- Objective: Verify all transformations were correctly applied.
-- Source:  bronze.erp_cust_az12
-- Target:  silver.erp_cust_az12
-- Convention: Every query MUST return 0 rows to pass.
--             Any row returned signals a defect in the transformation logic.
-- =============================================================================


-- =============================================================================
-- TEST 1: Referential Integrity — Customer ID (cid → cst_key)
-- Verifies: The 'NAS' prefix was stripped conditionally so every cid
--           now resolves to a known cst_key in silver.crm_cust_info.
--           Bronze had a direct-match failure; silver must have none.
-- PASS: 0 rows | FAIL: cid values that still don't match any cst_key
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE cid NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);


-- =============================================================================
-- TEST 2: Format Consistency — No residual 'NAS' prefix in cid
-- Verifies: The conditional strip was applied to ALL prefixed rows.
--           No row should still carry the raw 'NAS' leading characters.
-- PASS: 0 rows | FAIL: prefix was not stripped during transformation
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';


-- =============================================================================
-- TEST 3: Format Consistency — No unexpected cid formats
-- Verifies: After stripping, all cid values follow the expected pattern
--           of silver.crm_cust_info (e.g. 'CUST-XXXXXXX').
--           Catches any other rogue prefixes beyond 'NAS'.
-- PASS: 0 rows | FAIL: cid values with unrecognized formats remain
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE cid NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
)
AND cid NOT LIKE 'NAS%';  -- already caught by TEST 2, this isolates other formats


-- =============================================================================
-- TEST 4: Date Validity — No future birthdates
-- Verifies: bdate values beyond CURRENT_DATE were nullified.
--           Bronze had some future dates; silver must have none.
-- PASS: 0 rows | FAIL: future birthdates survived the transformation
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;


-- =============================================================================
-- TEST 5: Date Validity — No unreasonably old birthdates
-- Verifies: No birthdate is implausibly old (before 1900).
--           Catches cases where a bad date was converted instead of nullified.
-- PASS: 0 rows | FAIL: out-of-range dates were cast instead of nullified
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE bdate < '1900-01-01';


-- =============================================================================
-- TEST 6: Value Standardization — Gender only contains expected values
-- Verifies: All raw variants (M, F, Male, Female, NULL, empty, whitespace)
--           were mapped to exactly 'Male', 'Female', or 'n/a'.
--           No abbreviations, empty strings, or rogue values remain.
-- PASS: 0 rows | FAIL: unstandardized gen values survived
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Male', 'Female', 'Unknown')
   OR gen IS NULL;


-- =============================================================================
-- TEST 7: No NULL or empty cid values
-- Verifies: The prefix-stripping logic did not accidentally produce
--           empty strings (e.g. if a cid was exactly 'NAS' with nothing after).
-- PASS: 0 rows | FAIL: cid was over-stripped into an empty or null value
-- =============================================================================
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL
   OR TRIM(cid) = '';


-- =============================================================================
-- SUMMARY SCORECARD
-- Run this last — one row per test, failing_rows must be 0 for all.
-- =============================================================================
SELECT test_name, failing_rows,
       (CASE WHEN failing_rows = 0  THEN '✅ PASS' ELSE '❌ FAIL' END) AS status
FROM (
    SELECT 'TEST 1 – FK cid resolves to cst_key'       AS test_name,
       COUNT(*) AS failing_rows
    FROM silver.erp_cust_az12
    WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

    UNION ALL SELECT 'TEST 2 – No residual NAS prefix',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE cid LIKE 'NAS%'

    UNION ALL SELECT 'TEST 3 – No other unrecognized cid formats',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)
      AND cid NOT LIKE 'NAS%'

    UNION ALL SELECT 'TEST 4 – No future birthdates',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE bdate > CURRENT_DATE

    UNION ALL SELECT 'TEST 5 – No implausibly old birthdates',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE bdate < '1900-01-01'

    UNION ALL SELECT 'TEST 6 – Gender values standardized',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE gen NOT IN ('Male', 'Female', 'Unknown') OR gen IS NULL

    UNION ALL SELECT 'TEST 7 – No null or empty cid',
           COUNT(*)
    FROM silver.erp_cust_az12
    WHERE cid IS NULL OR TRIM(cid) = ''

    ORDER BY test_name
     ) AS checks;
