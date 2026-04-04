

SELECT cst_id,
       cst_key,
       TRIM(cst_firstname) AS cst_firstname, --remote unwanted spaces
       TRIM(cst_lastname) AS cst_lastname, --remove unwanted spaces
       CASE
           WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
           WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
           ELSE 'n/a'
       END AS cst_marital_status, --transform marital status into more readable and descriptive values
       CASE
           WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
           WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
           ELSE 'n/a'
       END AS cst_gndr,
       cst_create_date, --no issues found, no need of transformation
        NOW() AS _loaded_at
--generate a relation of rows after deduplication
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC, _loaded_at DESC --tie-breaker
            ) AS version_rank
    FROM bronze.crm_cust_info) AS subquery
WHERE version_rank = 1;
