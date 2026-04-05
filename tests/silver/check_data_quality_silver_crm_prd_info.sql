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
FROM silver.crm_prd_info;


-- -----------------------------------------------------------------------------
-- 2. Product Key Analysis & Cross-Source Integration
--    Goal: Validate prd_key consistency across CRM, ERP, and Sales sources
--          to establish reliable JOIN paths in the silver layer.
-- -----------------------------------------------------------------------------

--Expectation if data quality is good: standardized datas
SELECT DISTINCT prd_key
FROM silver.crm_prd_info;


SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details;


SELECT DISTINCT  cat_id
FROM silver.crm_prd_info;

SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2;




-- -----------------------------------------------------------------------------
-- 3. Product Name — Whitespace Check
--    Expectation if data is clean: No Results
-- -----------------------------------------------------------------------------

-- [GENERATOR] Run to regenerate check query if schema changes.
-- Copy output and replace the query below.
SELECT
    'SELECT prd_nm FROM silver.crm_prd_info WHERE ' ||
    STRING_AGG(
        column_name || ' != TRIM(' || column_name || ')',
        ' OR '
    ) AS check_query
FROM information_schema.columns
WHERE table_schema = 'silver'
  AND table_name   = 'crm_prd_info'
  AND column_name  = 'prd_nm'
  AND data_type    = 'text';

-- Expectation if data quality is good: no results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- -----------------------------------------------------------------------------
-- 4. Product Cost — NULL and Negative Value Check
--    Expectation if data is clean: No Results
-- -----------------------------------------------------------------------------
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;


-- -----------------------------------------------------------------------------
-- 5. Product Line — Distinct Values & Whitespace Check
-- -----------------------------------------------------------------------------

-- Expectation if data is clean: No Results
SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line);


-- -----------------------------------------------------------------------------
-- 6. Product Dates — Invalid Date Range Check
--    Expectation if data is clean: No Results

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

