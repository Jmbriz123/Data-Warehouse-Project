-- =============================================================================
-- Procedure : bronze.load_erp_data
-- Layer     : Bronze
-- Source    : datasets/source_erp/
-- Purpose   : Truncate and reload all raw ERP source tables into bronze layer
-- Run       : Manually or via scheduler after source CSV refresh
-- =============================================================================

CREATE OR REPLACE PROCEDURE bronze.load_erp_data(
    p_source_dir VARCHAR DEFAULT '/home/jemarco/repos/Data-Warehouse/datasets/source_erp'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time  TIMESTAMP := NOW();
    v_table_start TIMESTAMP;
    v_rows        INT       := 0;
BEGIN

    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> Starting ERP Bronze Load: %', v_start_time;
    RAISE NOTICE '==============================================';

    -- -------------------------------------------------------------------------
    -- 1. erp_cust_az12
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [1/3] Truncating bronze.erp_cust_az12...';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> [1/3] Loading bronze.erp_cust_az12...';
    EXECUTE format(
        'COPY bronze.erp_cust_az12
            (cid, bdate, gen)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/cust_az12.csv');

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.erp_cust_az12 SET _bronze_loaded_at = NOW();
    RAISE NOTICE '>> [1/3] Loaded % rows into bronze.erp_cust_az12 | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- 2. erp_loc_a101
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [2/3] Truncating bronze.erp_loc_a101...';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> [2/3] Loading bronze.erp_loc_a101...';
    EXECUTE format(
        'COPY bronze.erp_loc_a101
            (cid, cntry)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/loc_a101.csv'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.erp_loc_a101 SET _bronze_loaded_at = NOW();
    RAISE NOTICE '>> [2/3] Loaded % rows into bronze.erp_loc_a101 | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- 3. erp_px_cat_g1v2
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [3/3] Truncating bronze.erp_px_cat_g1v2...';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> [3/3] Loading bronze.erp_px_cat_g1v2...';
    EXECUTE format(
        'COPY bronze.erp_px_cat_g1v2
            (id, cat, subcat, maintenance)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/px_cat_g1v2.csv'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.erp_px_cat_g1v2 SET _bronze_loaded_at = NOW();
    RAISE NOTICE '>> [3/3] Loaded % rows into bronze.erp_px_cat_g1v2 | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- Done
    -- -------------------------------------------------------------------------
    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> ERP Bronze Load Completed.';
    RAISE NOTICE '>> Total Duration: % seconds',
        EXTRACT(EPOCH FROM (NOW() - v_start_time))::INT;
    RAISE NOTICE '==============================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '!! ERROR during ERP Bronze Load.';
        RAISE NOTICE '!! SQLERRM : %', SQLERRM;
        RAISE NOTICE '!! SQLSTATE: %', SQLSTATE;
        RAISE EXCEPTION 'bronze.load_erp_data failed — %', SQLERRM;
END;
$$;