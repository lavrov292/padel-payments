-- Migration: Add source column to tournaments table
-- 
-- INSTRUCTIONS:
-- 1. Execute this SQL in Supabase SQL Editor (NOT in sandboxed queries)
--    - Go to Supabase Dashboard -> SQL Editor
--    - Paste this entire file
--    - Click "Run"
--
-- 2. OR execute via psql:
--    psql $DATABASE_URL -f migrations/002_add_tournament_source.sql
--
-- This migration:
-- - Adds source column to tournaments table to track where tournament came from
-- - Default value 'manual' for existing tournaments
-- - New tournaments from Lunda will have source='lunda'

-- Add source column if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tournaments' AND column_name = 'source'
    ) THEN
        ALTER TABLE tournaments 
        ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
        
        -- Optional: Add constraint (comment out if Supabase complains)
        -- ALTER TABLE tournaments 
        -- ADD CONSTRAINT check_tournament_source 
        -- CHECK (source IN ('lunda', 'manual'));
    END IF;
END $$;








