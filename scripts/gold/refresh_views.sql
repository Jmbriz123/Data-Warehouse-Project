-- =============================================================
-- Purpose:  Refresh all gold layer materialized views to get fresh data
-- Order:    Dimensions first, fact table last
--           fact_sales depends on dim_products and dim_customers
--           being current before it refreshes
-- Method:   REFRESH CONCURRENTLY allows analysts to keep
--           querying during refresh — requires unique indexes
--           to exist (see gold_indexes.sql)
-- Requires: gold_indexes.sql must be run once before this
--           script is used for the first time
-- =============================================================


-- -------------------------------------------------------------
-- STEP 1: Refresh dimensions first
-- Order between dimensions doesn't matter
-- Both must complete before fact table refresh
-- -------------------------------------------------------------
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.dim_products;
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.dim_customers;


-- -------------------------------------------------------------
-- STEP 2: Refresh fact table last
-- Depends on dim_products and dim_customers being current
-- so surrogate key lookups resolve correctly
-- -------------------------------------------------------------
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.fact_sales;