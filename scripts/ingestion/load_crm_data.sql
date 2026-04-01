--wipe the table clean so you always start fresh, to avoid duplicate inserts
TRUNCATE TABLE bronze.crm_cust_info;

--bulk insert the data
COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname,cst_marital_status, cst_gndr, cst_create_date)
FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/cust_info.csv' -- Note to other person cloning this repo: Update this to your environment's path
WITH (FORMAT CSV,
    HEADER TRUE);



--wipe the table clean so you always start fresh, to avoid duplicate inserts
TRUNCATE TABLE bronze.crm_prd_info;

--bulk insert the data
COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/prd_info.csv' -- Note to other person cloning this repo: Update this to your environment's path
WITH (FORMAT CSV,
    HEADER TRUE);


--wipe the table clean so you always start fresh, to avoid duplicate inserts
TRUNCATE TABLE bronze.crm_sales_details;

--bulk insert the data
COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
FROM '/home/jemarco/repos/Data-Warehouse/datasets/source_crm/sales_details.csv' -- Note to other person cloning this repo: Update this to your environment's path
WITH (FORMAT CSV,
    HEADER TRUE);

