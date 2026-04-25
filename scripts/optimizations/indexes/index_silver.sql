-- =============================================================
-- Purpose:  Index silver layer JOIN columns used to build gold
--           layer star schema (dim_products, dim_customers,
--           fact_sales)
-- Context:  Batch pipeline — indexes created after silver load
--           completes, before gold build begins. No concurrent
--           access during this window so CONCURRENTLY is
--           intentionally omitted.
-- Related:  layers/gold/dim_products.sql
--           layers/gold/dim_customers.sql
--           layers/gold/fact_sales.sql
-- =============================================================


-- -------------------------------------------------------------
-- DIM_PRODUCTS BUILD INDEXES
-- Join: crm_prd_info.cat_id → erp_px_cat_g1v2.id
-- Filter: prd_end_dt for active product records (SCD Type 2)
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_silver_crm_prd_info_cat_id
    ON silver.crm_prd_info(cat_id);

CREATE INDEX IF NOT EXISTS idx_silver_erp_px_cat_g1v2_id
    ON silver.erp_px_cat_g1v2(id);

CREATE INDEX IF NOT EXISTS idx_silver_crm_prd_info_prd_end_dt
    ON silver.crm_prd_info(prd_end_dt);


-- -------------------------------------------------------------
-- DIM_CUSTOMERS BUILD INDEXES
-- Join: crm_cust_info.cst_key → erp_cust_az12.cid
--       crm_cust_info.cst_key → erp_loc_a101.cid
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_silver_crm_cust_info_cst_key
    ON silver.crm_cust_info(cst_key);

CREATE INDEX IF NOT EXISTS idx_silver_erp_cust_az12_cid
    ON silver.erp_cust_az12(cid);

CREATE INDEX IF NOT EXISTS idx_silver_erp_loc_a101_cid
    ON silver.erp_loc_a101(cid);


-- -------------------------------------------------------------
-- FACT_SALES BUILD INDEXES
-- Join: crm_sales_details.sls_prd_key → crm_prd_info.prd_key
--       crm_sales_details.sls_cust_id → crm_cust_info.cst_id
-- Filter: sls_order_dt for incremental loads
-- -------------------------------------------------------------
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