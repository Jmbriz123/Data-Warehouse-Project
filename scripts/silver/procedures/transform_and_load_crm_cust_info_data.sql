-- =============================================================================
-- Procedure : silver.transform_and_load_crm_cust_info
-- Layer      : Bronze → Silver (CRM customer dimension)
-- SLA        : Full-refresh / truncate-and-insert pattern
-- =============================================================================

CREATE OR REPLACE PROCEDURE silver.transform_and_load_crm_cust_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted  BIGINT  := 0;
    v_rows_rejected  BIGINT  := 0;
    v_start_time     TIMESTAMPTZ := clock_timestamp();
    v_proc_name      TEXT    := 'silver.transform_and_load_crm_cust_info';
BEGIN

    -- -------------------------------------------------------------------------
    -- 1. Audit: signal pipeline start
    -- -------------------------------------------------------------------------
    RAISE NOTICE '[%] [%] Starting procedure', v_proc_name, v_start_time;

    -- -------------------------------------------------------------------------
    -- 2. Pre-load data quality check: halt early on unexpected source state
    -- -------------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM bronze.crm_cust_info LIMIT 1) THEN
        RAISE EXCEPTION '[%] Source table bronze.crm_cust_info is empty. Aborting load.', v_proc_name;
    END IF;

    -- -------------------------------------------------------------------------
    -- 3. Capture rejected-row count BEFORE truncating target
    --    (preserves existing silver data if this run would be unusable)
    -- -------------------------------------------------------------------------
    SELECT COUNT(*)
    INTO   v_rows_rejected
    FROM   bronze.crm_cust_info
    WHERE  cst_id IS NULL;

    IF v_rows_rejected > 0 THEN
        RAISE WARNING '[%] % row(s) with NULL cst_id will be excluded from load.',
            v_proc_name, v_rows_rejected;
    END IF;

    -- -------------------------------------------------------------------------
    -- 4. Idempotent full-refresh (truncate → insert as a single transaction)
    -- -------------------------------------------------------------------------
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        _source_system,
        _bronze_loaded_at,
        _silver_loaded_at
    )
    SELECT
        src.cst_id,
        src.cst_key,

        -- Standardise: strip leading/trailing whitespace
        TRIM(src.cst_firstname)                                  AS cst_firstname,
        TRIM(src.cst_lastname)                                   AS cst_lastname,

        -- Conform: expand coded values to human-readable domain labels
        CASE UPPER(TRIM(src.cst_marital_status))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE          'Unknown'                               -- explicit sentinel
        END                                                      AS cst_marital_status,

        CASE UPPER(TRIM(src.cst_gndr))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE          'Unknown'                               -- explicit sentinel
        END                                                      AS cst_gndr,

        src.cst_create_date,

        -- Lineage: where did this record come from?
        'CRM'                                                    AS _source_system,

        -- Lineage: when was the raw record written to bronze?
        src._bronze_loaded_at                                           AS _bronze_loaded_at,

        -- Lineage: when was this silver record produced?
        clock_timestamp()                                        AS _silver_loaded_at

    FROM (
        -- Deduplication: keep the most-recent version of each customer
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC, _bronze_loaded_at DESC
            ) AS _row_rank
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL          -- push predicate inside subquery
    ) AS src
    WHERE src._row_rank = 1;

    -- -------------------------------------------------------------------------
    -- 5. Capture inserted-row count for observability
    -- -------------------------------------------------------------------------
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- -------------------------------------------------------------------------
    -- 6. Audit: signal pipeline completion with metrics
    -- -------------------------------------------------------------------------
    RAISE NOTICE '[%] Completed. Inserted: % | Rejected (NULL cst_id): % | Duration: %',
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
-- Grant execute rights to the ETL service role only (principle of least privilege)
-- =============================================================================
-- GRANT EXECUTE ON PROCEDURE silver.transform_and_load_crm_cust_info() TO etl_service_role;