-- =============================================================================
-- Migration: Fix Date Types with Explicit Conversion
-- =============================================================================

ALTER TABLE silver.crm_sales_details
    ALTER COLUMN sls_order_dt TYPE DATE
    USING (CASE WHEN sls_order_dt <= 0 THEN NULL ELSE sls_order_dt::TEXT::DATE END);

ALTER TABLE silver.crm_sales_details
    ALTER COLUMN sls_ship_dt TYPE DATE
    USING (CASE WHEN sls_ship_dt <= 0 THEN NULL ELSE sls_ship_dt::TEXT::DATE END);

ALTER TABLE silver.crm_sales_details
    ALTER COLUMN sls_due_dt TYPE DATE
    USING (CASE WHEN sls_due_dt <= 0 THEN NULL ELSE sls_due_dt::TEXT::DATE END);