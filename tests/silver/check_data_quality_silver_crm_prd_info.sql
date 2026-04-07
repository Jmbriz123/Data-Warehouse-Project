-- =============================================================================
-- SILVER LAYER: Quality Gate Tests — crm_prd_info
-- Objective: Verify all transformations were correctly applied.
-- Source:  bronze.crm_prd_info
-- Target:  silver.crm_prd_info
-- Convention: Every query MUST return 0 rows / 0 counts to pass.
--             Any row returned signals a defect in the transformation logic.
-- =============================================================================


-- =============================================================================
-- TEST 1: Primary Key Integrity — prd_id
-- Verifies: No NULL primary keys and no duplicate records exist.
--           A clean silver table must have exactly one unique row per product.
-- PASS: null_prd_ids = 0 AND total_records = total_unique_records
-- FAIL: Any NULLs or duplicates indicate a broken deduplication or load step
-- =============================================================================
SELECT
    COUNT(*)               AS total_records,
    COUNT(DISTINCT prd_id) AS total_unique_records,
    COUNT(*)
        - COUNT(DISTINCT prd_id)           AS duplicate_records,
    COUNT(CASE WHEN prd_id IS NULL THEN 1
          END)                             AS null_prd_ids
FROM silver.crm_prd_info;
-- PASS condition: duplicate_records = 0 AND null_prd_ids = 0


-- =============================================================================
-- TEST 2: Referential Integrity — prd_key vs crm_sales_details
-- Verifies: Every product key referenced in silver sales exists in silver
--           products. Orphaned sales rows indicate a join path is broken.
-- PASS: 0 rows | FAIL: sales rows reference a product that does not exist
-- =============================================================================
SELECT *
FROM silver.crm_sales_details s
WHERE s.sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_prd_info
);


-- =============================================================================
-- TEST 3: Referential Integrity — cat_id vs erp_px_cat_g1v2
-- Verifies: Every cat_id in silver.crm_prd_info resolves to a known category
--           in the ERP source. A mismatch breaks the category JOIN path.
-- PASS: 0 rows | FAIL: product rows with an unresolvable category ID
-- =============================================================================
SELECT *
FROM silver.crm_prd_info
WHERE cat_id NOT IN (
    SELECT id
    FROM bronze.erp_px_cat_g1v2
);


-- =============================================================================
-- TEST 4: Text Quality — No leading or trailing whitespace in prd_nm
-- Verifies: Product names were trimmed during transformation.
--           Untrimmed names cause silent JOIN mismatches and display issues.
-- PASS: 0 rows | FAIL: product names with surrounding whitespace remain
-- =============================================================================
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- =============================================================================
-- TEST 5: Text Quality — No leading or trailing whitespace in prd_line
-- Verifies: Product line values were trimmed during transformation.
-- PASS: 0 rows | FAIL: product line values with surrounding whitespace remain
-- =============================================================================
SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line);


-- =============================================================================
-- TEST 6: Numeric Validity — prd_cost must not be NULL or negative
-- Verifies: Invalid cost values were either corrected or nullified.
--           No product in silver should carry a negative or missing cost.
-- PASS: 0 rows | FAIL: products with invalid cost values survived
-- =============================================================================
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;


-- =============================================================================
-- TEST 7: Date Logic — Product start date must not exceed end date
-- Verifies: The date range on every product is chronologically valid.
--           prd_end_dt < prd_start_dt is a source system logic bug that
--           must be caught and handled before reaching silver.
-- PASS: 0 rows | FAIL: products with an inverted date range survived
-- =============================================================================
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- =============================================================================
-- SUMMARY SCORECARD
-- Run this last — one row per test, failing_rows must be 0 for all.
-- Note: TEST 1 is excluded here as it is a count-based check, not row-based.
--       Run TEST 1 separately and verify manually.
-- =============================================================================
SELECT 'TEST 2 – FK prd_key resolves in sales'         AS test_name,
        COUNT(*) AS failing_rows,
        CASE WHEN failing_rows= 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

UNION ALL SELECT 'TEST 3 – FK cat_id resolves in ERP categories',
        COUNT(*)
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT id FROM bronze.erp_px_cat_g1v2)

UNION ALL SELECT 'TEST 4 – No whitespace in prd_nm',
        COUNT(*)
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

UNION ALL SELECT 'TEST 5 – No whitespace in prd_line',
        COUNT(*)
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line)

UNION ALL SELECT 'TEST 6 – No NULL or negative prd_cost',
        COUNT(*)
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

UNION ALL SELECT 'TEST 7 – No inverted product date ranges',
        COUNT(*)
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

ORDER BY test_name;