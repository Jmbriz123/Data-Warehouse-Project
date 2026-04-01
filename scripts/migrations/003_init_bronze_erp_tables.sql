-- Drop first to ensure the schema matches the code perfectly
DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;


CREATE TABLE IF NOT EXISTS bronze.erp_CUST_AZ12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS bronze.erp_LOC_A101(
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze.erp_PX_CAT_G1V2(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);