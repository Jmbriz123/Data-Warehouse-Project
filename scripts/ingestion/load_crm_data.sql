CREATE OR REPLACE PROCEDURE bronze.load_crm_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Load Customer Info
    RAISE NOTICE '>> Truncating and Loading: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER TRUE);

    -- 2. Load Product Info
    RAISE NOTICE '>> Truncating and Loading: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER TRUE);

    -- 3. Load Sales Details
    RAISE NOTICE '>> Truncating and Loading: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER TRUE);

    RAISE NOTICE '>> CRM Data Load Completed Successfully.';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERROR: CRM Data Load failed. Rolling back changes.';
        RAISE EXCEPTION 'Detail: %', SQLERRM;
END;
$$;

CALL bronze.load_crm_data();