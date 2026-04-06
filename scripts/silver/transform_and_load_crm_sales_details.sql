BEGIN;
TRUNCATE  TABLE silver.crm_sales_details;
--load transformed data into silver layer
INSERT INTO  silver.crm_sales_details (sls_ord_num,
                                       sls_prd_key,
                                       sls_cust_id,
                                       sls_order_dt,
                                       sls_ship_dt,
                                       sls_due_dt,
                                       sls_sales,
                                       sls_quantity,
                                       sls_price)

--extract and transform bronze.crm_sales_details
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    (CASE
        WHEN sls_order_dt = 0  or LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE sls_order_dt::TEXT::DATE END) AS sls_order_dt,
    (CASE
        WHEN sls_ship_dt = 0  or LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE sls_ship_dt::TEXT::DATE END) AS sls_ship_dt,
    (CASE
        WHEN sls_due_dt = 0  or LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE sls_due_dt::TEXT::DATE END) AS sls_due_dt,
    (CASE
        WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity*ABS(sls_price)
        ELSE sls_sales END)AS sls_sales,
    sls_quantity,
    (CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales/ (CASE WHEN sls_quantity= 0 THEN NULL ELSE sls_quantity END)
        ELSE sls_price END) AS sls_price
FROM bronze.crm_sales_details;
COMMIT;
--Business Rules: IF sales is negative, zero, or null, derive it using quantity and price
--                If price is zero or null, derive it using sales and quantity
--                If price is negative, convert it to a positive value
-- Sales, price, and quuantity fields are related. If one field has invalid value that violates business rules: derive it using the other 2 fields