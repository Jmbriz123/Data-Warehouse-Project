-- Drop first to ensure the schema matches the code perfectly
DROP TABLE IF EXISTS silver.erp_cust_az12 CASCADE;
DROP TABLE IF EXISTS silver.erp_loc_a101 CASCADE;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2 CASCADE;


CREATE TABLE IF NOT EXISTS silver.erp_cust_az12(
    cid TEXT,
    bdate DATE,
    gen TEXT,


    -- Audit metadata (not from source CSV, added by pipeline)
    _bronze_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _silver_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system TEXT DEFAULT 'ERP'
);


CREATE TABLE IF NOT EXISTS silver.erp_loc_a101(
    cid TEXT,
    cntry TEXT,

    -- Audit metadata (not from source CSV, added by pipeline)
    _bronze_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _silver_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system TEXT DEFAULT 'ERP'
);

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2(
    id TEXT,
    cat TEXT,
    subcat TEXT,
    maintenance TEXT,


    -- Audit metadata (not from source CSV, added by pipeline)
    _bronze_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _silver_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system TEXT DEFAULT 'ERP'
);