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
    v_rows        INT       := 0;
BEGIN

    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> Starting CRM Bronze Load: %', v_start_time;
    RAISE NOTICE '==============================================';

    -- -------------------------------------------------------------------------
    -- 1. erp_cust_az12
    -- -------------------------------------------------------------------------
    RAISE NOTICE '>> [1/3] Truncating bronze.erp_cust_az12...';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> [1/3] Loading bronze.erp_cust_az12...';
    EXECUTE format(
        'COPY bronze.crm_cust_info
            (cid, bdate, gen)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/cust_az12');

    --get diagnostic data from recent executed SQL code
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '>> [1/3] Loaded % rows into bronze.cust_az12', v_rows;

    -- Stamp when this row was loaded (audit metadata)
    UPDATE bronze.erp_cust_az12 SET _loaded_at = NOW();

    -- -------------------------------------------------------------------------
    -- 2. erp_loc_a101
    -- -------------------------------------------------------------------------
    RAISE NOTICE '>> [2/3] Truncating bronze.erp_loc_a101...';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> [2/3] Loading bronze.erp_loc_a101...';
    EXECUTE format(
        'COPY bronze.crm_prd_info
            (cid, cntry)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/erp_loc_a101'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '>> [2/3] Loaded % rows into bronze.erp_loc_a101', v_rows;

    UPDATE bronze.erp_loc_a101 SET _loaded_at = NOW();

    -- -------------------------------------------------------------------------
    -- 3. erp_px_cat_g1v2
    -- -------------------------------------------------------------------------
    RAISE NOTICE '>> [3/3] Truncating bronze.erp_px_cat_g1v2..';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> [3/3] Loading bronze.erp_px_cat_g1v2..';
    EXECUTE format(
        'COPY bronze.crm_sales_details
            (id, cat, subcat, maintenance)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/erp_px_cat_g1v2'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '>> [3/3] Loaded % rows into bronze.erp_px_cat_g1v2', v_rows;

    UPDATE bronze.erp_px_cat_g1v2 SET _loaded_at = NOW();

    -- -------------------------------------------------------------------------
    -- Done
    -- -------------------------------------------------------------------------
    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> CRM Bronze Load Completed.';
    RAISE NOTICE '>> Duration: % seconds',
        EXTRACT(EPOCH FROM (NOW() - v_start_time))::INT;
    RAISE NOTICE '==============================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '!! ERROR during CRM Bronze Load.';
        RAISE NOTICE '!! SQLERRM : %', SQLERRM;
        RAISE NOTICE '!! SQLSTATE: %', SQLSTATE;
        RAISE EXCEPTION 'bronze.load_crm_data failed — %', SQLERRM;
END;
$$;
