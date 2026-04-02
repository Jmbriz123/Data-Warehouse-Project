-- =============================================================================
-- Script    : run_bronze_load.sql
-- Layer     : Bronze
-- Purpose   : Execute the full bronze layer data load
-- Usage     : psql -U postgres -d <your_db> -f scripts/bronze/run_bronze_load.sql
-- Requires  : All migrations must be run first
--             scripts/migrations/001_init_medallion_schema.sql
--             scripts/migrations/002_init_bronze_crm_tables.sql
--             scripts/migrations/003_init_bronze_erp_tables.sql
-- =============================================================================

-- HOW TO PASS YOUR LOCAL PATH:
-- Replace the path below with the absolute path to your source_crm folder.
-- Example (Linux/Mac) : '/home/yourname/repos/Data-Warehouse/datasets/source_crm'
-- Example (Windows)   : 'C:/Users/yourname/repos/Data-Warehouse/datasets/source_crm'
-- Leave it empty to use the default path set in the procedure definition.

CALL bronze.load_crm_data(
    p_source_dir => '/home/jemarco/repos/Data-Warehouse/datasets/source_crm'
);

CALL bronze.load_erp_data(
    p_source_dir => '/home/jemarco/repos/Data-Warehouse/datasets/source_erp'
     );

--validate

SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;

SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_px_cat_g1v2;
