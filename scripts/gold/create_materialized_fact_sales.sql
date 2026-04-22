/*
===============================================================================
Fact Table: gold.fact_sales
Description:
    - Centralizes sales transactions from the CRM system.
    - Links to Product and Customer dimensions via Surrogate Keys.
    - Standardizes column names and provides audit metadata.
===============================================================================
*/

BEGIN;

-- 1. Ensure clean recreation
DROP MATERIALIZED VIEW IF EXISTS gold.fact_sales CASCADE;

-- 2. Create the View
CREATE MATERIALIZED VIEW gold.fact_sales AS
SELECT
    -- Business Keys
    sd.sls_ord_num  AS order_number,

    -- Surrogate Keys (Mapping to Dimension Keys)
    -- Using COALESCE to point to a '-1' (Unknown) record if a join fails
    COALESCE(pr.product_key, -1)  AS product_key,
    COALESCE(cu.customer_key, -1) AS customer_key,

    -- Date Attributes (Ensuring standard naming)
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    -- Quantitative Measures
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price,

    -- Audit Columns
    CURRENT_TIMESTAMP AS _gold_processed_at,
    'CRM'             AS _source_system

FROM silver.crm_sales_details AS sd
-- Join with Dimensions to pull the SURROGATE KEYS (product_key, customer_key)
-- rather than the raw IDs (product_number, customer_id)
LEFT JOIN gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id; -- Joining on the Business Key

COMMIT;