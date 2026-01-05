-- Migration: Add tournament_type and update admin views
-- Execute this in Supabase SQL Editor

-- A) Add tournament_type column to tournaments table
ALTER TABLE tournaments 
ADD COLUMN IF NOT EXISTS tournament_type TEXT NOT NULL DEFAULT 'personal';

-- Add constraint for valid values (optional, but recommended)
ALTER TABLE tournaments 
ADD CONSTRAINT check_tournament_type 
CHECK (tournament_type IN ('personal', 'team'));

-- B) Drop and recreate admin_entries_view with new fields
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
    e.active,
    e.telegram_notified,
    e.telegram_notified_at,
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
    t.organizer
FROM entries e
JOIN players p ON e.player_id = p.id
JOIN tournaments t ON e.tournament_id = t.id;

-- C) Update admin_tournaments_view to include tournament_type
DROP VIEW IF EXISTS admin_tournaments_view;

CREATE VIEW admin_tournaments_view AS
SELECT 
    t.id AS tournament_id,
    t.title,
    t.starts_at,
    t.price_rub,
    t.tournament_type,
    t.source_last_updated,
    COUNT(e.id) AS entries_total,
    COUNT(e.id) FILTER (WHERE e.payment_status = 'paid') AS entries_paid,
    COUNT(e.id) FILTER (WHERE e.payment_status = 'pending') AS entries_pending
FROM tournaments t
LEFT JOIN entries e ON e.tournament_id = t.id
GROUP BY t.id, t.title, t.starts_at, t.price_rub, t.tournament_type, t.source_last_updated;

-- D) Create view for last sync time
CREATE OR REPLACE VIEW admin_last_sync AS
SELECT 
    MAX(t.source_last_updated) AS last_sync,
    COUNT(*) FILTER (WHERE t.source_last_updated IS NOT NULL) AS tournaments_with_sync
FROM tournaments t;

