/*
===============================================================================
Dimension: gold.dim_products
Description:
    - Provides a Star Schema dimension for products for the Gold Layer.
===============================================================================
*/
--create product dimensions
BEGIN;
DROP MATERIALIZED VIEW  IF EXISTS  gold.dim_products CASCADE;
CREATE MATERIALIZED VIEW gold.dim_products AS
SELECT
      ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
      pn.prd_id AS product_id,
      pn.prd_key AS product_number,
      pn.prd_nm AS product_name,
      pn.cat_id AS category_id,
      pc.cat AS category,
      pc.subcat AS subcategory,
      pn.prd_cost AS cost,
      pn.prd_line AS product_line,
      pn.prd_start_dt AS start_date,
      pc.maintenance,
     -- Audit Columns
    CURRENT_TIMESTAMP AS _gold_processed_at,
    'CRM + ERP'             AS _source_system
FROM silver.crm_prd_info as pn
        LEFT JOIN silver.erp_px_cat_g1v2 AS pc
                  ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;
--filter to only keep the current data, because the product table contains historical data
COMMIT;
