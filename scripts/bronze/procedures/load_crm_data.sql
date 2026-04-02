-- =============================================================================
-- Procedure : bronze.load_crm_data
-- Layer     : Bronze
-- Source    : datasets/source_crm/
-- Purpose   : Truncate and reload all raw CRM source tables into bronze layer
-- Run       : Manually or via scheduler after source CSV refresh
-- =============================================================================

CREATE OR REPLACE PROCEDURE bronze.load_crm_data(
    p_source_dir VARCHAR DEFAULT '/home/jemarco/repos/Data-Warehouse/datasets/source_crm'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time  TIMESTAMP := NOW();
    v_table_start TIMESTAMP;
    v_rows        INT       := 0;
BEGIN

    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> Starting CRM Bronze Load: %', v_start_time;
    RAISE NOTICE '==============================================';

    -- -------------------------------------------------------------------------
    -- 1. crm_cust_info
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [1/3] Truncating bronze.crm_cust_info...';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> [1/3] Loading bronze.crm_cust_info...';
    EXECUTE format(
        'COPY bronze.crm_cust_info
            (cst_id, cst_key, cst_firstname, cst_lastname,
             cst_marital_status, cst_gndr, cst_create_date)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/cust_info.csv'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.crm_cust_info SET _loaded_at = NOW();
    RAISE NOTICE '>> [1/3] Loaded % rows into bronze.crm_cust_info | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- 2. crm_prd_info
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [2/3] Truncating bronze.crm_prd_info...';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> [2/3] Loading bronze.crm_prd_info...';
    EXECUTE format(
        'COPY bronze.crm_prd_info
            (prd_id, prd_key, prd_nm, prd_cost,
             prd_line, prd_start_dt, prd_end_dt)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/prd_info.csv'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.crm_prd_info SET _loaded_at = NOW();
    RAISE NOTICE '>> [2/3] Loaded % rows into bronze.crm_prd_info | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- 3. crm_sales_details
    -- -------------------------------------------------------------------------
    v_table_start := NOW();
    RAISE NOTICE '>> [3/3] Truncating bronze.crm_sales_details...';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> [3/3] Loading bronze.crm_sales_details...';
    EXECUTE format(
        'COPY bronze.crm_sales_details
            (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
             sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
         FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        p_source_dir || '/sales_details.csv'
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE bronze.crm_sales_details SET _loaded_at = NOW();
    RAISE NOTICE '>> [3/3] Loaded % rows into bronze.crm_sales_details | Duration: % seconds',
        v_rows, EXTRACT(EPOCH FROM (NOW() - v_table_start))::INT;

    -- -------------------------------------------------------------------------
    -- Done
    -- -------------------------------------------------------------------------
    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> CRM Bronze Load Completed.';
    RAISE NOTICE '>> Total Duration: % seconds',
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