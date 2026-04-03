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
-- Expectation: NO ROWS returned
SELECT
    cst_id,
    COUNT(*) AS id_occurrences
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;