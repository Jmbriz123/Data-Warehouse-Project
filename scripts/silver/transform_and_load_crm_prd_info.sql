TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
--apply data transformations
SELECT prd_id,
       REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id, --new derived column from prd_key. Replace - with _ for data format consistency across tables
       SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
       prd_nm,
       (CASE
           WHEN prd_cost IS NULL THEN 0
           ELSE prd_cost END) AS prd_cost,
       (CASE UPPER(TRIM(prd_line))
           WHEN 'M' THEN 'Mountain'
           WHEN  'R' THEN 'Road'
           WHEN 'S' THEN 'Other Sales'
           WHEN 'T' THEN 'Touring'
           ELSE 'n/a' END
           ) AS prd_line,
       prd_start_dt,
       LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )  -1 AS prd_end_dt
FROM bronze.crm_prd_info;