-- Update admin_entries_view to include active and last_seen_in_source fields
-- This allows admin to distinguish between active entries (from latest import) 
-- and inactive entries (paid but not in latest import)

DROP VIEW IF EXISTS admin_entries_view;

CREATE OR REPLACE VIEW admin_entries_view AS
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
    e.paid_at,
    e.paid_amount_rub,
    e.paid_for_entry_id,
    e.paid_by_entry_id,
    e.payment_scope,
    e.first_seen_in_source,
    e.last_seen_in_source,
    p.full_name AS player_name,
    p.telegram_id,
    t.title AS tournament_title,
    t.location AS tournament_location,
    t.starts_at AS tournament_starts_at,
    t.price_rub AS tournament_price_rub,
    t.tournament_type,
    t.archived_at AS tournament_archived_at,
    t.source_last_updated
FROM entries e
JOIN players p ON e.player_id = p.id
JOIN tournaments t ON e.tournament_id = t.id
WHERE t.archived_at IS NULL;
