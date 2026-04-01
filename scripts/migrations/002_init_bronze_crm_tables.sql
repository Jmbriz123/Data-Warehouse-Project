DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE;
DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE;
DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;



CREATE TABLE IF NOT EXISTS bronze.crm_cust_info(
    cst_id INTEGER,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);


CREATE TABLE IF NOT EXISTS bronze.crm_prd_info(
    prd_id INTEGER,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INTEGER,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);


CREATE TABLE IF NOT EXISTS bronze.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INTEGER,
    sls_order_dt INTEGER,
    sls_ship_dt INTEGER,
    sls_due_dt  INTEGER,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price INTEGER
);