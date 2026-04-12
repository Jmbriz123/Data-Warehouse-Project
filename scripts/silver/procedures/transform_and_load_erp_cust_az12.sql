-- =============================================================================
-- Procedure : silver.transform_and_load_erp_cust_az12
-- Layer      : Bronze → Silver (ERP customer table)
-- Pattern    : Full-refresh / truncate-and-insert

-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_erp_cust_az12()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT      := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT        := 'silver.transform_and_load_erp_cust_az12';
BEGIN

    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        (CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid END)  AS cid,
        (CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate END) AS bdate,
        (CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
            ELSE 'Unknown' END) AS gen
    FROM bronze.erp_cust_az12;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    RAISE NOTICE '[%] Completed. Inserted: % | Duration: %',
        v_proc_name,
        v_rows_inserted,
        clock_timestamp() - v_start_time;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '[%] Procedure failed: % — %',
            v_proc_name, SQLSTATE, SQLERRM;
END;
$$;

