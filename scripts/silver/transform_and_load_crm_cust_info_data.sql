-- load transformed cust_info data into silver layer
BEGIN;

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        _loaded_at
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname)                          AS cst_firstname,
        TRIM(cst_lastname)                           AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END                                          AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END                                          AS cst_gndr,
        cst_create_date,
        NOW()                                        AS _loaded_at
    FROM (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC, _loaded_at DESC
            ) AS version_rank
        FROM bronze.crm_cust_info
    ) AS subquery
    WHERE version_rank = 1;

COMMIT;