-- Drop first to ensure the schema matches the code perfectly
DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;


CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12(
    cid TEXT,
    bdate DATE,
    gen TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);


CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101(
    cid TEXT,
    cntry TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2(
    id TEXT,
    cat TEXT,
    subcat TEXT,
    maintenance TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _loaded_at          TIMESTAMP
);
