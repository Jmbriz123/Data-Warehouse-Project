-- =============================================================================
-- Script Purpose: Exploration of data quality issues on bronze.erp_loc_a101
--                 before transforming and loading data into the silver layer.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Referential Integrity — Customer ID (cid vs cst_key)
--    Objective: Determine if bronze cid values can be reliably matched
--               to silver.crm_cust_info.cst_key.
-- -----------------------------------------------------------------------------

-- 1a. Preview both key columns side by side for visual comparison
SELECT cid FROM bronze.erp_loc_a101 ORDER BY cid;
SELECT cst_key FROM silver.crm_cust_info ORDER BY cst_key;
-- Realization: Inconsistent formats — bronze.erp_loc_a101.cid contains an
--              extra '-' prefix not present in silver.crm_cust_info.cst_key.

-- 1b. Validate if format normalization resolves the mismatch
--     Expectation if formatting is the only issue: No Results
SELECT *
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);
-- Realization: No referential integrity violations found. The delimiter
--              mismatch is purely a formatting issue.
--              Transformation rule: REPLACE(cid, '-', '') during silver load.


-- -----------------------------------------------------------------------------
-- 2. Country Field — Distinct Value & Consistency Check
--    Expectation if data is clean: Single standardized value per country,
--                                  no NULLs, no empty strings.
-- -----------------------------------------------------------------------------
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;
-- Realization: Three data quality issues found:
--              1. NULL values present.
--              2. Empty string values present.
--              3. Inconsistent naming (e.g. 'USA' vs 'United States').
--              Transformation rule: CASE WHEN standardization +
--              COALESCE/NULLIF handling required during silver load.