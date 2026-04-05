-- =============================================================================
-- Script Purpose: Exploration of data quality issues on bronze.crm_prd_info
--                 before transforming and loading data into the silver layer.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Primary Key Validation
--    Expectation: No NULLs, total_records = total_unique_records
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)              AS total_records,
    COUNT(DISTINCT prd_id) AS total_unique_records,
    COUNT(CASE WHEN prd_id IS NULL THEN 1 END) AS total_null_records
FROM bronze.crm_prd_info;


-- -----------------------------------------------------------------------------
-- 2. Product Key Analysis & Cross-Source Integration
--    Goal: Validate prd_key consistency across CRM, ERP, and Sales sources
--          to establish reliable JOIN paths in the silver layer.
-- -----------------------------------------------------------------------------

-- 2a. Analyze prd_key structure in the product master
--     Realization: prd_key contains a prefix that acts as a foreign key to
--     erp_px_cat_g1v2. Extraction via SUBSTRING/SPLIT_PART will be required.
SELECT DISTINCT prd_key
FROM bronze.crm_prd_info;

-- 2b. Validate format consistency for category reference
--     Realization: Integration friction identified — CRM/Sales uses '_' as
--     delimiter while ERP Category uses '-'.
--     Transformation rule: REPLACE('-', '_') required during silver load.
SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2;

-- 2c. Verify referential integrity in transactional data
--     Realization: sls_prd_key matches the prd_key format in crm_prd_info,
--     confirming a reliable JOIN path between Sales and Product dimensions.
SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details;

-- Overall realization: prd_key is a composite key that must be split into
-- a category prefix and a product key to enable joins across sources.
-- Format normalization ('_' vs '-') is mandatory before the silver load.


-- -----------------------------------------------------------------------------
-- 3. Product Name — Whitespace Check
--    Expectation if data is clean: No Results
-- -----------------------------------------------------------------------------

-- [GENERATOR] Run to regenerate check query if schema changes.
-- Copy output and replace the query below.
SELECT
    'SELECT prd_nm FROM bronze.crm_prd_info WHERE ' ||
    STRING_AGG(
        column_name || ' != TRIM(' || column_name || ')',
        ' OR '
    ) AS check_query
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name   = 'crm_prd_info'
  AND column_name  = 'prd_nm'
  AND data_type    = 'text';

-- [CHECK] Generated on: <date> | Column: prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- -----------------------------------------------------------------------------
-- 4. Product Cost — NULL and Negative Value Check
--    Expectation if data is clean: No Results
--    Realization: No negative values found, but NULL values are present.
--                 Transformation rule: COALESCE(prd_cost, 0) during silver load.
-- -----------------------------------------------------------------------------
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;


-- -----------------------------------------------------------------------------
-- 5. Product Line — Distinct Values & Whitespace Check
--    Realization: Values are abbreviated (e.g. 'M', 'R', 'S', 'T').
--                 CASE WHEN transformation required during silver load.
-- -----------------------------------------------------------------------------
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Expectation if data is clean: No Results
SELECT prd_line
FROM bronze.crm_prd_info
WHERE prd_line != TRIM(prd_line);


-- -----------------------------------------------------------------------------
-- 6. Product Dates — Invalid Date Range Check
--    Expectation if data is clean: No Results
--    Realization: Multiple records found where end date precedes start date.
--                 Transformation rule: date boundary correction required
--                 during silver load.
-- -----------------------------------------------------------------------------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;