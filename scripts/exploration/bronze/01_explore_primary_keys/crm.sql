-- =====================================================
-- Table: bronze.crm_cust_info
-- Purpose: Validate candidate primary key (cst_id)
-- =====================================================
-- 1. Check total vs distinct vs nulls
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT cst_id) AS total_unique_records,
    SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS total_null_records
FROM bronze.crm_cust_info;

-- Expected:
-- total_records = total_unique_records
-- total_null_records = 0

-- 2. Identify violations (duplicates or nulls)
--Expectation if the data is already clean: No Results
SELECT
    cst_id,
    COUNT(*) AS id_occurrences
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--3. identify duplicate cst_id records in bronze using row_number window function
--Expectation if the data is already clean: No Results
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC, _loaded_at DESC --tie-breaker
            ) AS version_rank
FROM bronze.crm_cust_info) AS sub
WHERE version_rank != 1;

-- -4. Check for unwanted and redundant whitespaces
-- Expectation if the data is already clean: No Results

-- [GENERATOR] Run this first to regenerate the check query if schema changes.
-- Copy its output and replace the query below.
SELECT
    'SELECT * FROM bronze.crm_cust_info WHERE ' ||
    STRING_AGG(
        column_name || ' != TRIM(' || column_name || ')',
        ' OR '
    ) AS check_query
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name   = 'crm_cust_info'
  AND data_type = 'text';

-- [CHECK]  | Columns: cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr
SELECT * FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)
   OR cst_firstname != TRIM(cst_firstname)
   OR cst_lastname != TRIM(cst_lastname)
   OR cst_marital_status != TRIM(cst_marital_status)
   OR cst_gndr != TRIM(cst_gndr);