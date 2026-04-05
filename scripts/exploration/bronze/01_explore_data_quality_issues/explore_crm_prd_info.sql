--Script Purpose: Exploration of data quality issues on bronze.crm_prd_info before transforming and loading the data into silver layer

--1. Explore primary key violations: not unique and/or NULL
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT prd_id) AS total_unique_records,
       COUNT(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS total_null_records
FROM bronze.crm_prd_info;
