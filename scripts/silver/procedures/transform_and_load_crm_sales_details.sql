-- =============================================================================
-- Procedure : silver.transform_and_load_crm_sales_details
-- Layer      : Bronze → Silver (CRM sales fact table)
-- Pattern    : Full-refresh / truncate-and-insert

-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_crm_sales_details()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT      := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT        := 'silver.transform_and_load_crm_sales_details';
BEGIN

    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (sls_ord_num,
                                          sls_prd_key,
                                          sls_cust_id,
                                          sls_order_dt,
                                          sls_ship_dt,
                                          sls_due_dt,
                                          sls_sales,
                                          sls_quantity,
                                          sls_price)
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        (CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE sls_order_dt::TEXT::DATE END) AS sls_order_dt,
        (CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE sls_ship_dt::TEXT::DATE END) AS sls_ship_dt,
        (CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE sls_due_dt::TEXT::DATE END) AS sls_due_dt,
        (CASE
            WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales END) AS sls_sales,
        sls_quantity,
        (CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / (CASE WHEN sls_quantity = 0 THEN NULL ELSE sls_quantity END)
            ELSE sls_price END) AS sls_price
    FROM bronze.crm_sales_details;

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

-- GRANT EXECUTE ON PROCEDURE silver.transform_and_load_crm_sales_details() TO etl_service_role;