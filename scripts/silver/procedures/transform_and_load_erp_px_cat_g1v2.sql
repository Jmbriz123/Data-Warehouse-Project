-- =============================================================================
-- Procedure : silver.transform_and_load_erp_px_cat_g1v2
-- Layer      : Bronze → Silver (ERP product category table)
-- Pattern    : Full-refresh / truncate-and-insert
-- Note       : No transformations applied — source data quality is clean
-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_erp_px_cat_g1v2()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT      := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT        := 'silver.transform_and_load_erp_px_cat_g1v2';
BEGIN

    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

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
