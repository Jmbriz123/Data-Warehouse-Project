--load extracted data into silver layer
BEGIN;
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (id,
                                    cat,
                                    subcat,
                                    maintenance)
-- extract data (no need of transformation, data quality is already good from the source)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id IN(
    SELECT cat_id
    FROM silver.crm_prd_info
    );
COMMIT;


