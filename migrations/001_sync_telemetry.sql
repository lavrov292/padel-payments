-- Migration: Add sync telemetry for import tracking
-- 
-- INSTRUCTIONS:
-- 1. Execute this SQL in Supabase SQL Editor (NOT in sandboxed queries)
--    - Go to Supabase Dashboard -> SQL Editor
--    - Paste this entire file
--    - Click "Run"
--
-- 2. OR execute via psql:
--    psql $DATABASE_URL -f migrations/001_sync_telemetry.sql
--
-- This migration:
-- - Adds first_seen_in_source and last_seen_in_source to entries table
-- - Creates sync_runs table for tracking import statistics
-- - Creates admin_last_sync_view for easy access to last sync info

-- A) Add first_seen_in_source and last_seen_in_source to entries if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'first_seen_in_source'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN first_seen_in_source TIMESTAMPTZ NOT NULL DEFAULT NOW();
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'last_seen_in_source'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN last_seen_in_source TIMESTAMPTZ NOT NULL DEFAULT NOW();
    END IF;
END $$;

-- B) Create sync_runs table
CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'lunda',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    tournaments_upsert INT NOT NULL DEFAULT 0,
    players_upsert INT NOT NULL DEFAULT 0,
    entries_new INT NOT NULL DEFAULT 0,
    entries_existing INT NOT NULL DEFAULT 0,
    entries_deleted INT NOT NULL DEFAULT 0,
    entries_inactivated INT NOT NULL DEFAULT 0,
    error TEXT,
    json_path TEXT,
    json_mtime TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- C) Create view for last sync
CREATE OR REPLACE VIEW admin_last_sync_view AS
SELECT 
    id,
    source,
    started_at,
    finished_at,
    tournaments_upsert,
    players_upsert,
    entries_new,
    entries_existing,
    entries_deleted,
    entries_inactivated,
    error,
    json_path,
    json_mtime,
    created_at
FROM sync_runs
ORDER BY started_at DESC
LIMIT 1;

