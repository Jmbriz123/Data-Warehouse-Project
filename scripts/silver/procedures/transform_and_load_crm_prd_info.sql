-- =============================================================================
-- Procedure : silver.transform_and_load_crm_prd_info
-- Layer      : Bronze → Silver (CRM product dimension with SCD Type 2 dates)
-- Pattern    : Full-refresh / truncate-and-insert
-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_crm_prd_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT      := 0;
    v_rows_rejected  BIGINT      := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT        := 'silver.transform_and_load_crm_prd_info';
BEGIN

    -- -------------------------------------------------------------------------
    -- 1. Audit: signal pipeline start
    -- -------------------------------------------------------------------------
    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    -- -------------------------------------------------------------------------
    -- 2. Data quality gate: halt if source is empty
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM bronze.crm_prd_info LIMIT 1) THEN
        RAISE EXCEPTION '[%] Source table bronze.crm_prd_info is empty. Aborting load.',
            v_proc_name;
    END IF;

    -- -------------------------------------------------------------------------
    -- 3. Capture rows with critical quality issues before touching the target
    -- -------------------------------------------------------------------------
    SELECT COUNT(*)
    INTO   v_rows_rejected
    FROM   bronze.crm_prd_info
    WHERE  prd_id  IS NULL
       OR  prd_key IS NULL
       OR  LENGTH(TRIM(prd_key)) < 7;   -- key too short to derive both columns

    IF v_rows_rejected > 0 THEN
        RAISE WARNING '[%] % row(s) have a NULL or malformed prd_key and will be excluded.',
            v_proc_name, v_rows_rejected;
    END IF;

    -- -------------------------------------------------------------------------
    -- 4. Idempotent full-refresh — truncate + insert in one transaction
    -- -------------------------------------------------------------------------
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        _source_system,
        _bronze_loaded_at,
        _silver_loaded_at
    )
    SELECT
        prd_id,

        -- Derived column: extract the category segment from prd_key
        -- prd_key format: "XX-XX-<rest>"  →  positions 1-5 become cat_id
        -- Normalise separator: replace '-' with '_' for cross-table consistency
        REPLACE(
            SUBSTRING(TRIM(prd_key), 1, 5),
            '-', '_'
        )                                                   AS cat_id,

        -- Derived column: strip the leading category prefix (chars 1-6)
        -- leaving only the product-specific segment of the key
        SUBSTRING(TRIM(prd_key), 7)                         AS prd_key,

        TRIM(prd_nm)                                        AS prd_nm,

        -- Null-safe default: missing cost treated as zero, not unknown
        COALESCE(prd_cost, 0)                               AS prd_cost,

        -- Conform: expand single-character codes to business-readable labels
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE          'Unknown'
        END                                                 AS prd_line,

        prd_start_dt,

        -- SCD Type 2 end date: the day before the next version's start date.
        -- LEAD() looks ahead within the same product key, ordered by version date.
        -- Subtracting INTERVAL '1 day' is type-safe and explicit (vs integer -1).
        -- NULL means the record is the currently active version.
        LEAD(prd_start_dt) OVER (
            PARTITION BY SUBSTRING(TRIM(prd_key), 7)
            ORDER BY     prd_start_dt ASC
        ) - INTERVAL '1 day'                                AS prd_end_dt,

        -- Lineage columns
        'CRM'                                               AS _source_system,
        _bronze_loaded_at                                          AS _bronze_loaded_at,
        clock_timestamp()                                   AS _silver_loaded_at

    FROM bronze.crm_prd_info
    WHERE prd_id  IS NOT NULL
      AND prd_key IS NOT NULL
      AND LENGTH(TRIM(prd_key)) >= 7;

    -- -------------------------------------------------------------------------
    -- 5. Capture DML row count for observability
    -- -------------------------------------------------------------------------
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- -------------------------------------------------------------------------
    -- 6. Audit: completion notice with metrics
    -- -------------------------------------------------------------------------
    RAISE NOTICE '[%] Completed. Inserted: % | Rejected: % | Duration: %',
        v_proc_name,
        v_rows_inserted,
        v_rows_rejected,
        clock_timestamp() - v_start_time;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '[%] Procedure failed: % — %',
            v_proc_name, SQLSTATE, SQLERRM;
END;
$$;

-- =============================================================================
-- GRANT EXECUTE ON PROCEDURE silver.transform_and_load_crm_prd_info() TO etl_service_role;
-- =============================================================================