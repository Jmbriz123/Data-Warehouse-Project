

--validate if the columns use to relate tables  match values
--expectation: no results
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN(
    SELECT prd_key
    FROM silver.crm_prd_info
    );

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_cust_info
    );
--realization: no issues found

-- =============================================================================
-- DATA PROFILING: Sales Date Columns
-- Objective: Ensure date strings are convertible to DATE type in Silver.
-- =============================================================================

-- 1. Check for Logical Consistency (e.g., Is order date after ship date?)
--Expectation: No Results
-- Observation: If order > ship, the source system has a logic bug.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;
--Realization: No issue found

-- 2. Check for "Out of Bounds" dates (e.g., Year 0001 or 9999)
-- =============================================================================
-- DATA PROFILING: Identifying "Zero-Date" Anomalies
-- =============================================================================

-- 1. Quantitative Audit: How many rows are impacted?
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN sls_order_dt <= 0 THEN 1 END) AS invalid_order_dates,
    COUNT(CASE WHEN sls_ship_dt <= 0 THEN 1 END) AS invalid_ship_dates,
    COUNT(CASE WHEN sls_due_dt <= 0 THEN 1 END) AS invalid_due_dates
FROM bronze.crm_sales_details;
--Realization: There are 17 invalid order dates
-- 2. Logical Audit: Are these '0' dates associated with specific products or years?
-- (This helps determine if the bug is from a specific source system or time period)
SELECT
    SUBSTRING(CAST(sls_order_dt AS TEXT), 1, 4) AS year_part,
    COUNT(*)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
GROUP BY 1;

-- 3. Check for NULLs in mandatory business fields
SELECT COUNT(*) AS missing_order_dates
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL;
--Realization: No missing order dates

-- =============================================================================
-- DATA PROFILING: Identifying invalid integers representing dates
-- =============================================================================
SELECT
    SUM((CASE WHEN LENGTH(sls_order_dt::text) != 8 THEN 1 ELSE 0 END)) AS invalid_order_dates,
    SUM((CASE WHEN LENGTH(sls_ship_dt::text) != 8 THEN 1 ELSE 0 END)) AS invalid_ship_dates,
    SUM((CASE WHEN LENGTH(sls_due_dt::text) != 8 THEN 1 ELSE 0 END)) AS invalid_due_dates
FROM bronze.crm_sales_details;
--realization: Order Dates have invalid values (digits !=8)

--determine what values violated the digit property
SELECT
    DISTINCT  sls_order_dt
FROM bronze.crm_sales_details
WHERE LENGTH(sls_order_dt::text) != 8;
--Realization: Invalid dates are the following: 0, 5489, 32154


-- =============================================================================
-- DATA PROFILING: Exploring sales, quantity, and price. Business Rules: SUM(sales) = quantity * price. No negative, zeros, and nulls are allowed
-- =============================================================================

SELECT DISTINCT crm_sales_details.sls_quantity
FROM bronze.crm_sales_details
ORDER BY sls_quantity;
-- realization: quantities are: 1,2,3,4,5,10

SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_sales IS NULL
    OR sls_price <=0 OR sls_price IS NULL
    or sls_quantity <=0 OR sls_price IS NULL;
--realization: there are some invalid sales and price values

SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != (ABS(sls_quantity) * ABS(sls_price)); --use absolute values just to check if the product of values after transformation (turning negatives to positive) will follow the business rule
--realization: some violate the business rule: sales = quantity * price

--Business Rules: IF sales is negative, zero, or null, derive it using quantity and price
--                If price is zero or null, derive it using sales and quantity
--                If price is negative, convert it to a positive value