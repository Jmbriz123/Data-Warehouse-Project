/*
===============================================================================
Dimension: gold.dim_customers
Description:
    - Consolidates Customer Master data from CRM, ERP, and Location sources.
    - Resolves gender conflicts by prioritizing CRM but falling back to ERP.
    - Provides a Star Schema dimension for the Gold Layer.
===============================================================================
*/

BEGIN;

-- 1. Use CASCADE to ensure clean recreation if other views depend on this

DROP MATERIALIZED VIEW IF EXISTS gold.dim_customers CASCADE;

-- 2. Create the View with explicit column aliases and business-friendly names
CREATE MATERIALIZED VIEW gold.dim_customers AS
SELECT
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    -- Business Keys & IDs
    ci.cst_id       AS customer_id,
    ci.cst_key      AS customer_number,

    -- Descriptive Attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

    CASE
        WHEN ci.cst_gndr IN ('Male', 'Female', 'M', 'F') THEN ci.cst_gndr
        WHEN ca.gen     IN ('Male', 'Female', 'M', 'F') THEN ca.gen
        ELSE 'Unknown'
    END AS gender,

    -- Consistent Naming and Data Cleaning
    ca.bdate            AS birth_date,
    ci.cst_create_date  AS created_at,

    -- Audit Columns (Standard for Professional Pipelines)
    CURRENT_TIMESTAMP   AS _gold_processed_at,
    'CRM + ERP'         AS _source_systems

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

COMMIT;

