-- =============================================================
-- Script:   gold_indexes.sql
-- Purpose:  Index gold materialized views for:
--           Analyst query performance (attribute indexes)
-- Context:  Run ONCE on first setup. Indexes survive REFRESH.
--           No drop/recreate needed — REFRESH handles rebuild.
-- Note:     CONCURRENTLY omitted — batch pipeline, no
--           concurrent access during index creation window
-- =============================================================


-- -------------------------------------------------------------
-- DIM_PRODUCTS
-- -------------------------------------------------------------

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_dim_products_product_key
    ON gold.dim_products(product_key);

-- Analyst drill-down attributes
CREATE INDEX IF NOT EXISTS idx_gold_dim_products_category
    ON gold.dim_products(category);

CREATE INDEX IF NOT EXISTS idx_gold_dim_products_subcategory
    ON gold.dim_products(subcategory);

CREATE INDEX IF NOT EXISTS idx_gold_dim_products_product_line
    ON gold.dim_products(product_line);


-- -------------------------------------------------------------
-- DIM_CUSTOMERS
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_customer_key
    ON gold.dim_customers(customer_key);

-- Analyst filter attributes
CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_country
    ON gold.dim_customers(country);

CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_gender
    ON gold.dim_customers(gender);

CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_marital_status
    ON gold.dim_customers(marital_status);


-- -------------------------------------------------------------
-- FACT_SALES
-- -------------------------------------------------------------


-- If a customer can buy the same product multiple times
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_fact_sales_pk
    ON gold.fact_sales(customer_key, product_key, order_number);

-- FK joins — every BI query joins on these
CREATE INDEX IF NOT EXISTS idx_gold_fact_sales_product_key
    ON gold.fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_gold_fact_sales_customer_key
    ON gold.fact_sales(customer_key);

-- Date filters — analysts always filter by these
CREATE INDEX IF NOT EXISTS idx_gold_fact_sales_order_date
    ON gold.fact_sales(order_date);

CREATE INDEX IF NOT EXISTS idx_gold_fact_sales_shipping_date
    ON gold.fact_sales(shipping_date);