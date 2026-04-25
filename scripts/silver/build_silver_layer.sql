-- =============================================================
-- Silver Layer Pipeline
-- Pattern: TRUNCATE + INSERT inside each procedure means
--          indexes are reset for free. Drop-recreate is
--          intentionally omitted — redundant in this pattern.
-- =============================================================

-- -------------------------------------------------------------
-- STEP 1: Load silver tables
-- Each procedure internally: TRUNCATE → INSERT
-- Indexes reset for free on every TRUNCATE
-- -------------------------------------------------------------
CALL silver.transform_and_load_crm_cust_info();
CALL silver.transform_and_load_crm_prd_info();
CALL silver.transform_and_load_crm_sales_details();
CALL silver.transform_and_load_erp_cust_az12();
CALL silver.transform_and_load_erp_loc_a101();
CALL silver.transform_and_load_erp_px_cat_g1v2();


-- -------------------------------------------------------------
-- STEP 2: Create indexes for gold build
-- IF NOT EXISTS = safe to re-run, skips if already exists
-- Created AFTER load so insert performance is unaffected
-- -------------------------------------------------------------

-- DIM_PRODUCTS
-- Join: crm_prd_info.cat_id → erp_px_cat_g1v2.id
-- Filter: prd_end_dt for active product records (SCD Type 2)
CREATE INDEX IF NOT EXISTS idx_silver_crm_prd_info_cat_id
    ON silver.crm_prd_info(cat_id);

CREATE INDEX IF NOT EXISTS idx_silver_erp_px_cat_g1v2_id
    ON silver.erp_px_cat_g1v2(id);

CREATE INDEX IF NOT EXISTS idx_silver_crm_prd_info_prd_end_dt
    ON silver.crm_prd_info(prd_end_dt);


-- DIM_CUSTOMERS
-- Join: crm_cust_info.cst_key → erp_cust_az12.cid
--       crm_cust_info.cst_key → erp_loc_a101.cid
CREATE INDEX IF NOT EXISTS idx_silver_crm_cust_info_cst_key
    ON silver.crm_cust_info(cst_key);

CREATE INDEX IF NOT EXISTS idx_silver_erp_cust_az12_cid
    ON silver.erp_cust_az12(cid);

CREATE INDEX IF NOT EXISTS idx_silver_erp_loc_a101_cid
    ON silver.erp_loc_a101(cid);


-- FACT_SALES
-- Join: crm_sales_details.sls_prd_key → crm_prd_info.prd_key
--       crm_sales_details.sls_cust_id → crm_cust_info.cst_id
-- Filter: sls_order_dt for incremental loads
CREATE INDEX IF NOT EXISTS idx_silver_crm_prd_info_prd_key
    ON silver.crm_prd_info(prd_key);

CREATE INDEX IF NOT EXISTS idx_silver_crm_sales_details_sls_prd_key
    ON silver.crm_sales_details(sls_prd_key);

CREATE INDEX IF NOT EXISTS idx_silver_crm_cust_info_cst_id
    ON silver.crm_cust_info(cst_id);

CREATE INDEX IF NOT EXISTS idx_silver_crm_sales_details_sls_cust_id
    ON silver.crm_sales_details(sls_cust_id);

CREATE INDEX IF NOT EXISTS idx_silver_crm_sales_details_sls_order_dt
    ON silver.crm_sales_details(sls_order_dt);