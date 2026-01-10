-- Migration: Add paid_amount_rub column to entries table
-- 
-- INSTRUCTIONS:
-- 1. Execute this SQL in Supabase SQL Editor (NOT in sandboxed queries)
--    - Go to Supabase Dashboard -> SQL Editor
--    - Paste this entire file
--    - Click "Run"
--
-- 2. OR execute via psql:
--    psql $DATABASE_URL -f migrations/003_add_paid_amount.sql
--
-- This migration:
-- - Adds paid_amount_rub column to entries to store actual payment amount
-- - This allows showing correct payment amount in notifications (e.g., 3500 for half payment in team tournament)

-- Add paid_amount_rub column if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'paid_amount_rub'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN paid_amount_rub NUMERIC(10, 2) NULL;
        
        COMMENT ON COLUMN entries.paid_amount_rub IS 'Actual amount paid for this entry (may differ from tournament.price_rub for team tournaments)';
    END IF;
END $$;








