-- =============================================================================
-- DATA PROFILING: bronze.erp_cust_az12
-- Objective: Identify data quality issues before loading into silver layer.
-- Table: bronze.erp_cust_az12
-- Related silver table: silver.crm_cust_info
-- =============================================================================


-- =============================================================================
-- SECTION 1: Referential Integrity — Customer ID (cid vs cst_key)
-- Objective: Determine if bronze cid values can be matched to silver cst_key.
-- =============================================================================

-- 1a. Preview both key columns side by side for visual comparison
SELECT cid FROM bronze.erp_cust_az12 ORDER BY cid;
SELECT cst_key FROM silver.crm_cust_info ORDER BY cst_key;

-- 1b. Direct match — how many cid values have NO match in silver?
-- Expectation: 0 rows (full referential integrity)
SELECT *
FROM bronze.erp_cust_az12
WHERE cid NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);
-- Realization: Most cid values fail the direct match.
--              cid contains extra leading characters (e.g. 'NAS') not present in cst_key.

-- 1c. Substring match — does stripping the first 3 characters resolve the mismatch?
-- Objective: Confirm the extra characters are the only formatting difference.
-- Expectation: All rows match after trimming
SELECT *
FROM bronze.erp_cust_az12
WHERE SUBSTRING(cid, 4, LENGTH(cid::TEXT)) IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);
-- Realization: Rows match after stripping the first 3 characters,
--              confirming the issue is purely a leading-character prefix.

-- 1d. Format audit — how widespread is the 'NAS' prefix?
-- Objective: Determine if the prefix is consistent or mixed across rows.
SELECT 'has NAS prefix'    AS format_type, COUNT(*) AS row_count FROM bronze.erp_cust_az12 WHERE cid LIKE 'NAS%'
UNION ALL
SELECT 'no NAS prefix',                    COUNT(*) FROM bronze.erp_cust_az12 WHERE cid NOT LIKE 'NAS%';
-- Realization: cid format is inconsistent — some rows carry the 'NAS' prefix,
--              others do not. Transformation must conditionally strip it.


-- =============================================================================
-- SECTION 2: Date Validity — Birthdate (bdate)
-- Objective: Identify out-of-range birthdates that are logically impossible.
-- =============================================================================

-- 2a. Future birthdates — no customer can be born in the future
-- Expectation: 0 rows
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate > CURRENT_DATE;
-- Realization: Some bdate values are set in the future.
--              These must be nullified during transformation.

-- 2b. Quantify the impact
SELECT
    COUNT(*)                                          AS total_rows,
    COUNT(CASE WHEN bdate > CURRENT_DATE THEN 1 END) AS future_bdates,
    COUNT(CASE WHEN bdate IS NULL        THEN 1 END) AS null_bdates
FROM bronze.erp_cust_az12;


-- =============================================================================
-- SECTION 3: Value Standardization — Gender (gen)
-- Objective: Identify all distinct raw values to define the mapping rules
--            needed during transformation.
-- =============================================================================

-- 3a. Full distinct value audit including NULLs and whitespace
SELECT
    COALESCE(NULLIF(TRIM(gen), ''), '[empty or whitespace]') AS gen_value,
    COUNT(*) AS row_count
FROM bronze.erp_cust_az12
GROUP BY 1
ORDER BY 2 DESC;
-- Realization: gen contains mixed representations:
--              abbreviations (M/F), full words (Male/Female),
--              NULLs, and empty/whitespace strings.
--              Transformation must map all variants to a single standard (e.g. 'Male'/'Female'/'n/a').


-- =============================================================================
-- SECTION 4: Summary — Data Quality Issues Found
-- =============================================================================
-- | Column | Issue                                      | Action in Silver         |
-- |--------|--------------------------------------------|--------------------------|
-- | cid    | Inconsistent 'NAS' prefix on some rows     | Strip prefix conditionally|
-- | bdate  | Future dates present                       | Set to NULL              |
-- | gen    | Mixed formats, NULLs, empty strings        | Standardize to Male/Female/n/a |
-- =============================================================================