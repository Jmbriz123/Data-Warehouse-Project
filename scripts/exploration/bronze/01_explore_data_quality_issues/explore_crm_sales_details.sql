

--validate if the columns use to relate tables  match values
--expectation: no results
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_prd_info
    );

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_cust_info
    );
--realization: no issues found

