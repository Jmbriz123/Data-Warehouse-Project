DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE;
DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE;
DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;



CREATE TABLE IF NOT EXISTS bronze.crm_cust_info(
    cst_id INTEGER,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS bronze.crm_prd_info(
    prd_id INTEGER,
    prd_key TEXT,
    prd_nm TEXT,
    prd_cost INTEGER,
    prd_line TEXT,
    prd_start_dt DATE,
    prd_end_dt DATE,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS bronze.crm_sales_details(
    sls_ord_num TEXT,
    sls_prd_key TEXT,
    sls_cust_id INTEGER,
    sls_order_dt INTEGER,
    sls_ship_dt INTEGER,
    sls_due_dt  INTEGER,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price INTEGER,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);