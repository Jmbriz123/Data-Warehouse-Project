--Script Purpose: Exploration of data quality issues on bronze.crm_prd_info before transforming and loading the data into silver layer

--1. Explore primary key violations: not unique and/or NULL
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT prd_id) AS total_unique_records,
       COUNT(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS total_null_records
FROM bronze.crm_prd_info;

--2. Explore prd_key column values
--Realization : prd_key substring is actually the product category, which is used as primary key on erp_px_cat_g1v2
SELECT DISTINCT prd_key
FROM bronze.crm_prd_info;
--Explore if data format is consistent across tables
--Realization: format is inconsistent. One uses _ as separator, the other one uses -
SELECT id
FROM bronze.erp_px_cat_g1v2;