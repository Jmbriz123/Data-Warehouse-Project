-- =============================================================================
-- SILVER LAYER: Quality Gate Tests — crm_sales_details
-- Convention: Every query MUST return 0 rows to pass.
--             Any row returned signals a defect in the transformation logic.
-- =============================================================================


-- =============================================================================
-- TEST 1: Referential Integrity — Product key
-- Verifies: Every sls_prd_key in silver sales still resolves to a known product.
--           A broken join here means orphaned sales records slipped through.
-- PASS: 0 rows | FAIL: orphaned sales rows
-- =============================================================================
SELECT *
FROM silver.crm_sales_details s
WHERE s.sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_prd_info
);


-- =============================================================================
-- TEST 2: Referential Integrity — Customer ID
-- Verifies: Every sls_cust_id resolves to a known customer.
-- PASS: 0 rows | FAIL: sales records with no matching customer
-- =============================================================================
SELECT *
FROM silver.crm_sales_details s
WHERE s.sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_cust_info
);


-- =============================================================================
-- TEST 3: Date Logic — Order must precede ship date
-- Verifies: The chronological constraint holds after transformation.
--           In bronze this was confirmed clean; it must stay clean in silver.
-- PASS: 0 rows | FAIL: order date is later than ship date (logic regression)
-- =============================================================================
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;


-- =============================================================================
-- TEST 4: Date Completeness — No NULL order dates
-- Verifies: The bronze layer had no NULL order dates; silver must preserve this.
-- PASS: 0 rows | FAIL: nulls were introduced during transformation
-- =============================================================================
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt  IS NULL
   OR sls_due_dt   IS NULL;


-- =============================================================================
-- TEST 5: Date Format — All dates are valid DATE type values
-- Verifies: The invalid integers (0, 5489, 32154) from bronze were either
--           corrected to a valid date or set to NULL — never cast as garbage.
--           Silver stores dates as DATE, so this checks for out-of-range values.
-- PASS: 0 rows | FAIL: dates outside reasonable business range survived
-- =============================================================================
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt < '2000-01-01'
   OR sls_order_dt > CURRENT_DATE
   OR sls_ship_dt  < '2000-01-01'
   OR sls_ship_dt  > CURRENT_DATE
   OR sls_due_dt   < '2000-01-01'
   OR sls_due_dt   > CURRENT_DATE;


-- =============================================================================
-- TEST 6: Business Rule — No negative, zero, or NULL in sales, price, quantity
-- Verifies: The transformation rejected or corrected all invalid values.
--           Bronze had some; silver must have none.
-- PASS: 0 rows | FAIL: invalid values were not cleaned
-- =============================================================================
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales    <= 0 OR sls_sales    IS NULL
   OR sls_price    <= 0 OR sls_price    IS NULL
   OR sls_quantity <= 0 OR sls_quantity IS NULL;


-- =============================================================================
-- TEST 7: Business Rule — Sales = Quantity × Price (no exceptions)
-- Verifies: The derivation rule was applied correctly for every row.
--           In bronze, ABS() was used to probe the relationship;
--           in silver the values themselves must be positive, so no ABS() needed.
-- PASS: 0 rows | FAIL: derived sales figure does not match qty × price
-- =============================================================================
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;


-- =============================================================================
-- SUMMARY SCORECARD (run last — one row per test, count should be 0 for all)
-- =============================================================================
SELECT test_name, failing_rows,
      (CASE WHEN failing_rows = 0  THEN '✅ PASS' ELSE '❌ FAIL' END) AS status
FROM (
        SELECT
        'TEST 1 – FK product'         AS test_name,
        COUNT(*) AS failing_rows
    FROM silver.crm_sales_details
    WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

    UNION ALL SELECT
        'TEST 2 – FK customer',
        COUNT(*)
    FROM silver.crm_sales_details
    WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

    UNION ALL SELECT
        'TEST 3 – Order before ship date',
        COUNT(*)
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt

    UNION ALL SELECT
        'TEST 5 – Date in valid range',
        COUNT(*)
    FROM silver.crm_sales_details
    WHERE sls_order_dt < '2000-01-01' OR sls_order_dt > CURRENT_DATE
       OR sls_ship_dt  < '2000-01-01' OR sls_ship_dt  > CURRENT_DATE
       OR sls_due_dt   < '2000-01-01' OR sls_due_dt   > CURRENT_DATE

    UNION ALL SELECT
        'TEST 6 – No invalid sales/price/qty',
        COUNT(*)
    FROM silver.crm_sales_details
    WHERE sls_sales <= 0 OR sls_sales IS NULL
       OR sls_price <= 0 OR sls_price IS NULL
       OR sls_quantity <= 0 OR sls_quantity IS NULL

    UNION ALL SELECT
        'TEST 7 – Sales = qty × price',
        COUNT(*)
    FROM silver.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price

    ORDER BY test_name
     ) AS checks;
