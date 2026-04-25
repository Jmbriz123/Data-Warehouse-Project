-- =============================================================
-- Silver Layer Pipeline
-- Pattern: DROP indexes → TRUNCATE + INSERT (inside procedures)
--          → CREATE indexes
-- Rationale: Dropping indexes before bulk load avoids
--            incremental B-tree maintenance during INSERT.
--            CREATE INDEX after load builds the tree in one
--            sequential sorted pass — faster than row-by-row
--            index maintenance during insert.
-- =============================================================


-- -------------------------------------------------------------
-- STEP 1: Drop indexes before loading
-- Eliminates incremental B-tree maintenance during bulk INSERT
-- -------------------------------------------------------------

-- crm_prd_info indexes
DROP INDEX IF EXISTS idx_silver_crm_prd_info_cat_id;
DROP INDEX IF EXISTS idx_silver_crm_prd_info_prd_end_dt;
DROP INDEX IF EXISTS idx_silver_crm_prd_info_prd_key;

-- erp_px_cat_g1v2 indexes
DROP INDEX IF EXISTS idx_silver_erp_px_cat_g1v2_id;

-- crm_cust_info indexes
DROP INDEX IF EXISTS idx_silver_crm_cust_info_cst_key;
DROP INDEX IF EXISTS idx_silver_crm_cust_info_cst_id;

-- erp_cust_az12 indexes
DROP INDEX IF EXISTS idx_silver_erp_cust_az12_cid;

-- erp_loc_a101 indexes
DROP INDEX IF EXISTS idx_silver_erp_loc_a101_cid;

-- crm_sales_details indexes
DROP INDEX IF EXISTS idx_silver_crm_sales_details_sls_prd_key;
DROP INDEX IF EXISTS idx_silver_crm_sales_details_sls_cust_id;
DROP INDEX IF EXISTS idx_silver_crm_sales_details_sls_order_dt;


-- -------------------------------------------------------------
-- STEP 2: Load silver tables (no indexes = clean bulk insert)
-- Each procedure internally: TRUNCATE → INSERT
-- -------------------------------------------------------------
CALL silver.transform_and_load_crm_cust_info();
CALL silver.transform_and_load_crm_prd_info();
CALL silver.transform_and_load_crm_sales_details();
CALL silver.transform_and_load_erp_cust_az12();
CALL silver.transform_and_load_erp_loc_a101();
CALL silver.transform_and_load_erp_px_cat_g1v2();


-- -------------------------------------------------------------
-- STEP 3: Recreate indexes after load
-- BUILD INDEX scans table once, sorts in memory, writes
-- sequentially — always faster than incremental maintenance
-- -------------------------------------------------------------

-- DIM_PRODUCTS
-- Join: crm_prd_info.cat_id → erp_px_cat_g1v2.id
-- Filter: prd_end_dt for active product records (SCD Type 2)
CREATE INDEX idx_silver_crm_prd_info_cat_id
    ON silver.crm_prd_info(cat_id);

CREATE INDEX idx_silver_erp_px_cat_g1v2_id
    ON silver.erp_px_cat_g1v2(id);

CREATE INDEX idx_silver_crm_prd_info_prd_end_dt
    ON silver.crm_prd_info(prd_end_dt);


-- DIM_CUSTOMERS
-- Join: crm_cust_info.cst_key → erp_cust_az12.cid
--       crm_cust_info.cst_key → erp_loc_a101.cid
CREATE INDEX idx_silver_crm_cust_info_cst_key
    ON silver.crm_cust_info(cst_key);

CREATE INDEX idx_silver_erp_cust_az12_cid
    ON silver.erp_cust_az12(cid);

CREATE INDEX idx_silver_erp_loc_a101_cid
    ON silver.erp_loc_a101(cid);


-- FACT_SALES
-- Join: crm_sales_details.sls_prd_key → crm_prd_info.prd_key
--       crm_sales_details.sls_cust_id → crm_cust_info.cst_id
-- Filter: sls_order_dt for incremental loads
CREATE INDEX idx_silver_crm_prd_info_prd_key
    ON silver.crm_prd_info(prd_key);

CREATE INDEX idx_silver_crm_sales_details_sls_prd_key
    ON silver.crm_sales_details(sls_prd_key);

CREATE INDEX idx_silver_crm_cust_info_cst_id
    ON silver.crm_cust_info(cst_id);

CREATE INDEX idx_silver_crm_sales_details_sls_cust_id
    ON silver.crm_sales_details(sls_cust_id);

CREATE INDEX idx_silver_crm_sales_details_sls_order_dt
    ON silver.crm_sales_details(sls_order_dt);