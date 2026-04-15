-- =============================================================
-- Gold Layer: Star Schema Quality Tests
-- Fact  : gold.fact_sales
-- Dims  : gold.dim_products, gold.dim_customers
-- =============================================================

-- ---------------------------------------------------------------
-- 1. Referential integrity: orphaned product keys in fact
-- ---------------------------------------------------------------
SELECT 'Orphaned product_key in fact_sales' AS check_name, *
FROM gold.fact_sales AS sls
WHERE sls.product_key NOT IN (
    SELECT product_key FROM gold.dim_products
);

-- ---------------------------------------------------------------
-- 2. Referential integrity: orphaned customer keys in fact
-- ---------------------------------------------------------------
SELECT 'Orphaned customer_key in fact_sales' AS check_name, *
FROM gold.fact_sales AS sls
WHERE sls.customer_key NOT IN (
    SELECT customer_key FROM gold.dim_customers
);

-- ---------------------------------------------------------------
-- 3. NULL key checks: fact table must have no NULL keys
-- ---------------------------------------------------------------
SELECT 'NULL product_key in fact_sales' AS check_name, *
FROM gold.fact_sales
WHERE product_key IS NULL;

SELECT 'NULL customer_key in fact_sales' AS check_name, *
FROM gold.fact_sales
WHERE customer_key IS NULL;

-- ---------------------------------------------------------------
-- 4. Uniqueness: dimension PKs must not be duplicated
-- ---------------------------------------------------------------
SELECT 'Duplicate product_key in dim_products' AS check_name,
       product_key, COUNT(*) AS occurrences
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

SELECT 'Duplicate customer_key in dim_customers' AS check_name,
       customer_key, COUNT(*) AS occurrences
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------
-- 5. Summary Scoreboard
-- ---------------------------------------------------------------
SELECT
    check_name,
    failing_rows,
    CASE WHEN failing_rows = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (

    -- Referential integrity
    SELECT 'RI: fact_sales → dim_products' AS check_name,
           COUNT(*) AS failing_rows
    FROM gold.fact_sales
    WHERE product_key NOT IN (SELECT product_key FROM gold.dim_products)

    UNION ALL
    SELECT 'RI: fact_sales → dim_customers',
           COUNT(*)
    FROM gold.fact_sales
    WHERE customer_key NOT IN (SELECT customer_key FROM gold.dim_customers)

    -- NULL key checks
    UNION ALL
    SELECT 'NULL check: fact_sales.product_key',
           COUNT(*)
    FROM gold.fact_sales
    WHERE product_key IS NULL

    UNION ALL
    SELECT 'NULL check: fact_sales.customer_key',
           COUNT(*)
    FROM gold.fact_sales
    WHERE customer_key IS NULL

    -- Dimension uniqueness
    UNION ALL
    SELECT 'Uniqueness: dim_products.product_key',
           COUNT(*) - COUNT(DISTINCT product_key)
    FROM gold.dim_products

    UNION ALL
    SELECT 'Uniqueness: dim_customers.customer_key',
           COUNT(*) - COUNT(DISTINCT customer_key)
    FROM gold.dim_customers

) AS checks
ORDER BY status DESC, check_name;  -- FAILs bubble up first