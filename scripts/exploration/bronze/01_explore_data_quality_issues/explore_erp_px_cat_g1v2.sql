-- =============================================================================
-- Script Purpose: Exploration of data quality issues on bronze.erp_px_cat_g1v2
--                 before transforming and loading data into the silver layer.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Referential Integrity — Category ID (id vs cat_id)
--    Objective: Determine if bronze id values can be reliably matched
--               to silver.crm_prd_info.cat_id.
-- -----------------------------------------------------------------------------

-- 1a. Preview both key columns side by side for visual comparison
SELECT DISTINCT id     FROM bronze.erp_px_cat_g1v2  ORDER BY id;
SELECT DISTINCT cat_id FROM silver.crm_prd_info      ORDER BY cat_id;

-- 1b. Check for orphaned category records (categories with no matching product)
--     Expectation if data is clean: No Results
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT cat_id
    FROM silver.crm_prd_info
);
-- Realization: One category record exists in bronze.erp_px_cat_g1v2 that has
--              no matching product in silver.crm_prd_info. This is an orphaned
--              category and should be investigated before the silver load.


-- -----------------------------------------------------------------------------
-- 2. Category Field (cat) — Distinct Values & Whitespace Check
--    Expectation if data is clean: No Results on whitespace check
-- -----------------------------------------------------------------------------
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2
ORDER BY cat;
-- Realization: Four valid values found — Bikes, Accessories, Clothing,
--              Components. No unexpected values.

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);
-- Realization: No unwanted whitespace found.


-- -----------------------------------------------------------------------------
-- 3. Subcategory Field (subcat) — Distinct Values & Whitespace Check
--    Expectation if data is clean: No Results on whitespace check
-- -----------------------------------------------------------------------------
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
ORDER BY subcat;

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);
-- Realization: No unwanted whitespace found.


-- -----------------------------------------------------------------------------
-- 4. Maintenance Field — Distinct Values & Whitespace Check
--    Expectation if data is clean: Only 'Yes' and 'No', no whitespace issues
-- -----------------------------------------------------------------------------
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
ORDER BY maintenance;
-- Realization: Only two values present — 'Yes' and 'No'. No unexpected values.

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);
-- Realization: No unwanted whitespace found.