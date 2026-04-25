--executions of all the stored procedures in the silver layer
CALL silver.transform_and_load_crm_cust_info();
CALL silver.transform_and_load_crm_prd_info();
CALL silver.transform_and_load_crm_sales_details();
CALL silver.transform_and_load_erp_cust_az12();
CALL silver.transform_and_load_erp_loc_a101();
CALL silver.transform_and_load_erp_px_cat_g1v2();