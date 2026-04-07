-- =============================================================================
-- Load: bronze.erp_loc_a101 → silver.erp_loc_a101
-- Transformations:
--   cid  : strip '-' delimiter to align with silver.crm_cust_info.cst_key
--   cntry: standardize country names; normalize NULLs and empty strings to 'n/a'
-- =============================================================================
BEGIN;
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (cid, cntry)
-- apply data transformations
SELECT REPLACE(cid, '-', '') AS cid,
         (CASE
              WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
              WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
              WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'n/a'
              ELSE cntry END) AS cntry
 FROM bronze.erp_loc_a101;
END;