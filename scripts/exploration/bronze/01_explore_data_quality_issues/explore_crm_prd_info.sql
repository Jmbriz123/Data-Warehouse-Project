--Script Purpose: Exploration of data quality issues on bronze.crm_prd_info before transforming and loading the data into silver layer

--1. Explore primary key violations: not unique and/or NULL
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT prd_id) AS total_unique_records,
       COUNT(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS total_null_records
FROM bronze.crm_prd_info;


-- =============================================================================
-- DATA DISCOVERY: Product Key Relationships & Integration Logic
-- Goal: Validate 'prd_key' consistency across CRM, ERP, and Sales sources.
-- =============================================================================

-- 1. Analyze 'prd_key' structure in Product Master
-- Observation: 'prd_key' contains a prefix that acts as a foreign key
-- to 'erp_px_cat_g1v2'. Extraction logic (SUBSTRING/SPLIT_PART) will be required.
SELECT DISTINCT prd_key
FROM bronze.crm_prd_info;

-- 2. Validate format consistency for Category Reference
-- Observation: Integration Friction identified.
-- CRM/Sales uses '_' as a delimiter, while ERP Category uses '-'.
-- Transformation Rule: REPLACE('-' WITH '_') required during Silver migration.
SELECT id
FROM bronze.erp_px_cat_g1v2;

-- 3. Verify Referential Integrity in Transactional Data
-- Observation: Confirmed 'sls_prd_key' matches the 'prd_key' format in crm_prd_info.
-- This ensures a reliable JOIN path between Sales and Product dimensions.
SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details;

-- =============================================================================
-- OVERALL REALIZATION:
-- The 'prd_key' is a composite key. To join the Product Master with the ERP
-- Category table, a format normalization step is mandatory to align
-- mismatched delimiters ('_' vs '-').
-- ============================================================================= be divided into 2 parts: category and actual product key, to allow joining to another tables