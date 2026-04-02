-- Drop first to ensure the schema matches the code perfectly
DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;


CREATE TABLE IF NOT EXISTS bronze.erp_CUST_AZ12(
    CID TEXT,
    BDATE DATE,
    GEN TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);


CREATE TABLE IF NOT EXISTS bronze.erp_LOC_A101(
    CID TEXT,
    CNTRY TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.erp_PX_CAT_G1V2(
    ID TEXT,
    CAT TEXT,
    SUBCAT TEXT,
    MAINTENANCE TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);