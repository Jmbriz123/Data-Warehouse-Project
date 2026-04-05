-- =============================================================================
-- Migration: silver.crm_prd_info — add derived column cat_id
-- Reason: prd_key in bronze is can be further broken down into 2 parts: prd_key and cat_id. This is to add relationships across tables and to allow joins
-- =============================================================================
ALTER TABLE silver.crm_prd_info
    ADD COLUMN IF NOT EXISTS cat_id TEXT;