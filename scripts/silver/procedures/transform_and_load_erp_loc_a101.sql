-- =============================================================================
-- Procedure : silver.transform_and_load_erp_loc_a101
-- Layer      : Bronze → Silver (ERP location table)
-- Pattern    : Full-refresh / truncate-and-insert
-- Transformations:
--   cid  : strip '-' delimiter to align with silver.crm_cust_info.cst_key
--   cntry: standardize country names; normalize NULLs and empty strings to 'Unknown'
-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_erp_loc_a101()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT      := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT        := 'silver.transform_and_load_erp_loc_a101';
BEGIN

    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        (CASE
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) = 'DE'           THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IS NULL
              OR UPPER(TRIM(cntry)) = ''              THEN 'Unknown'
            ELSE cntry END) AS cntry
    FROM bronze.erp_loc_a101;

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
