-- =============================================================================
-- Script Purpose: Post-load data quality tests for silver.erp_loc_a101
--                 Validates that all transformations applied during the
--                 bronze → silver load produced the expected results.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Test 1. Referential Integrity — cid must match silver.crm_cust_info.cst_key
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);


-- -----------------------------------------------------------------------------
-- Test 2. cid Format — no '-' delimiter should remain after transformation
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT cid
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%';


-- -----------------------------------------------------------------------------
-- Test 3. Country — no NULLs, empty strings, or known abbreviations remaining
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT cntry
FROM silver.erp_loc_a101
WHERE cntry IS NULL
   OR TRIM(cntry) = ''
   OR UPPER(cntry) IN ('US', 'USA', 'DE');


-- -----------------------------------------------------------------------------
-- Test 4. Country — distinct values should only contain standardized names
--         Expectation: 'United States', 'Germany', 'n/a', and any ELSE values
-- -----------------------------------------------------------------------------
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;


-- -----------------------------------------------------------------------------
-- Test 5. Audit metadata — _loaded_at must be populated on every row
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_loc_a101
WHERE _loaded_at IS NULL;


-- =============================================================================
-- Scoreboard — summarized test results
-- Expectation: failed_count = 0 for all tests
-- =============================================================================
SELECT test, failing_rows,
    (CASE WHEN failing_rows = 0  THEN '✅ PASS' ELSE '❌ FAIL' END) AS status
FROM (
    SELECT
    'Test 1: Referential integrity (cid → cst_key)'   AS test,
    COUNT(*)                                           AS failing_rows
    FROM silver.erp_loc_a101
    WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

    UNION ALL

    SELECT
        'Test 2: cid format (no remaining dashes)'         AS test,
        COUNT(*)                                           AS failing_rows
    FROM silver.erp_loc_a101
    WHERE cid LIKE '%-%'

    UNION ALL

    SELECT
        'Test 3: Country normalization (no NULLs/abbrevs)' AS test,
        COUNT(*)                                           AS failing_rows
    FROM silver.erp_loc_a101
    WHERE cntry IS NULL
       OR TRIM(cntry) = ''
       OR UPPER(cntry) IN ('US', 'USA', 'DE')

    UNION ALL

    SELECT
        'Test 5: Audit column (_loaded_at not NULL)'       AS test,
        COUNT(*)                                           AS failing_rows
    FROM silver.erp_loc_a101
    WHERE _loaded_at IS NULL

    ORDER BY test
     ) AS checks;