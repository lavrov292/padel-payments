-- Migration: Fix tournament archiving logic (remove dependency on source column)
-- 
-- INSTRUCTIONS:
-- 1. Execute this SQL in Supabase SQL Editor (NOT in sandboxed queries)
--    - Go to Supabase Dashboard -> SQL Editor
--    - Paste this entire file
--    - Click "Run"
--
-- 2. OR execute via psql:
--    psql $DATABASE_URL -f migrations/004_fix_tournament_archiving.sql
--
-- This migration:
-- - Ensures last_seen_in_source and archived_at columns exist
-- - Updates admin views to filter by archived_at IS NULL (not by active)

-- A) Ensure columns exist (idempotent)
DO $$
BEGIN
    -- last_seen_in_source
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tournaments' AND column_name = 'last_seen_in_source'
    ) THEN
        ALTER TABLE tournaments 
        ADD COLUMN last_seen_in_source TIMESTAMPTZ NULL;
    END IF;
    
    -- archived_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tournaments' AND column_name = 'archived_at'
    ) THEN
        ALTER TABLE tournaments 
        ADD COLUMN archived_at TIMESTAMPTZ NULL;
    END IF;
END $$;

-- B) Update admin_tournaments_view to filter by archived_at IS NULL
DROP VIEW IF EXISTS admin_tournaments_view;

CREATE VIEW admin_tournaments_view AS
SELECT 
    t.id AS tournament_id,
    t.title,
    t.starts_at,
    t.price_rub,
    t.tournament_type,
    t.source_last_updated,
    t.active,
    t.archived_at,
    t.first_seen_in_source,
    t.last_seen_in_source,
    COUNT(e.id) AS entries_total,
    COUNT(e.id) FILTER (WHERE e.payment_status = 'paid') AS entries_paid,
    COUNT(e.id) FILTER (WHERE e.payment_status = 'pending') AS entries_pending
FROM tournaments t
LEFT JOIN entries e ON e.tournament_id = t.id
WHERE t.archived_at IS NULL  -- Only show non-archived tournaments
GROUP BY t.id, t.title, t.starts_at, t.price_rub, t.tournament_type, t.source_last_updated, t.active, t.archived_at, t.first_seen_in_source, t.last_seen_in_source
ORDER BY t.starts_at ASC;

-- C) Update admin_entries_view to filter by tournament archived_at IS NULL
DROP VIEW IF EXISTS admin_entries_view;

CREATE VIEW admin_entries_view AS
SELECT 
    e.id AS entry_id,
    e.tournament_id,
    e.player_id,
    e.payment_status,
    e.payment_id,
    e.payment_url,
    e.manual_paid,
    e.manual_note,
    e.active AS entry_active,
    e.telegram_notified,
    e.telegram_notified_at,
    e.first_seen_in_source,
    e.last_seen_in_source,
    p.full_name,
    p.telegram_id,
    t.title,
    t.location,
    t.starts_at,
    t.ends_at,
    t.price_rub,
    t.tournament_type,
    t.source_last_updated,
    t.active AS tournament_active,
    t.archived_at,
    t.first_seen_in_source AS tournament_first_seen_in_source,
    t.last_seen_in_source AS tournament_last_seen_in_source,
    t.organizer
FROM entries e
JOIN players p ON e.player_id = p.id
JOIN tournaments t ON e.tournament_id = t.id
WHERE t.archived_at IS NULL;  -- Only show entries for non-archived tournaments




