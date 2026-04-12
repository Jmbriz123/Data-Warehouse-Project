-- =============================================================================
-- Script Purpose: Post-load data quality tests for silver.erp_px_cat_g1v2
--                 Validates that all transformations applied during the
--                 bronze → silver load produced the expected results.
-- =============================================================================
-- -----------------------------------------------------------------------------
-- Test 0. Check primary key violations
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS total_recordS,
       COUNT(DISTINCT id) AS total_unique_primary_keys
FROM silver.erp_px_cat_g1v2;
-- -----------------------------------------------------------------------------
-- Test 1. Referential Integrity — id must match silver.crm_prd_info.cat_id
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT id
FROM silver.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT cat_id
    FROM silver.crm_prd_info
);


-- -----------------------------------------------------------------------------
-- Test 2. Category Field (cat) — no unwanted whitespace
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat);


-- -----------------------------------------------------------------------------
-- Test 3. Category Field (cat) — only valid domain values present
--         Expectation: Only 'Bikes', 'Accessories', 'Clothing', 'Components'
-- -----------------------------------------------------------------------------
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2
ORDER BY cat;


-- -----------------------------------------------------------------------------
-- Test 4. Subcategory Field (subcat) — no unwanted whitespace
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);


-- -----------------------------------------------------------------------------
-- Test 5. Subcategory Field (subcat) — distinct values audit
--         Expectation: All values are recognizable subcategories; no nulls
-- -----------------------------------------------------------------------------
SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2
ORDER BY subcat;


-- -----------------------------------------------------------------------------
-- Test 6. Maintenance Field — only valid domain values present
--         Expectation: Only 'Yes' and 'No'
-- -----------------------------------------------------------------------------
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2
ORDER BY maintenance;


-- -----------------------------------------------------------------------------
-- Test 7. Maintenance Field — no unwanted whitespace
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);


-- -----------------------------------------------------------------------------
-- Test 8. Maintenance Field — no unexpected values outside allowed domain
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE maintenance NOT IN ('Yes', 'No')
   OR maintenance IS NULL;


-- -----------------------------------------------------------------------------
-- Test 9. Audit metadata — _loaded_at must be populated on every row
--         Expectation: No Results
-- -----------------------------------------------------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE _silver_loaded_at IS NULL;


-- =============================================================================
-- Scoreboard — summarized test results
-- Expectation: failing_rows = 0 for all tests
-- =============================================================================
SELECT test, failing_rows,
    CASE WHEN failing_rows = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (

    -- Test 2: Whitespace — cat
    SELECT
        'Test 2: Whitespace in cat'                            AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE cat != TRIM(cat)

    UNION ALL

    -- Test 3: Invalid domain values — cat
    SELECT
        'Test 3: Invalid cat values'                           AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE cat NOT IN ('Bikes', 'Accessories', 'Clothing', 'Components')
       OR cat IS NULL

    UNION ALL

    -- Test 4: Whitespace — subcat
    SELECT
        'Test 4: Whitespace in subcat'                         AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE subcat != TRIM(subcat)

    UNION ALL

    -- Test 5: NULLs — subcat
    SELECT
        'Test 5: NULL subcat values'                           AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE subcat IS NULL

    UNION ALL

    -- Test 6: Whitespace — maintenance
    SELECT
        'Test 6: Whitespace in maintenance'                    AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE maintenance != TRIM(maintenance)

    UNION ALL

    -- Test 7: Invalid domain values — maintenance
    SELECT
        'Test 7: Invalid maintenance values'                   AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE maintenance NOT IN ('Yes', 'No')
       OR maintenance IS NULL

    UNION ALL

    -- Test 8: Audit column
    SELECT
        'Test 8: Audit column (_silver_loaded_at not NULL)'           AS test,
        COUNT(*)                                               AS failing_rows
    FROM silver.erp_px_cat_g1v2
    WHERE _silver_loaded_at IS NULL

) checks

ORDER BY test;

