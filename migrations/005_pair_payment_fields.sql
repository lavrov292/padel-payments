-- Migration: Add fields for pair payment tracking
-- Adds columns to track who paid for whom in team tournaments

DO $$
BEGIN
    -- Add paid_for_entry_id (for payer: which entry they paid for)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'paid_for_entry_id'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN paid_for_entry_id BIGINT NULL;
        
        -- Add foreign key constraint
        ALTER TABLE entries
        ADD CONSTRAINT fk_paid_for_entry 
        FOREIGN KEY (paid_for_entry_id) REFERENCES entries(id) ON DELETE SET NULL;
    END IF;
    
    -- Add paid_by_entry_id (for partner: who paid for them)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'paid_by_entry_id'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN paid_by_entry_id BIGINT NULL;
        
        -- Add foreign key constraint
        ALTER TABLE entries
        ADD CONSTRAINT fk_paid_by_entry 
        FOREIGN KEY (paid_by_entry_id) REFERENCES entries(id) ON DELETE SET NULL;
    END IF;
    
    -- Add payment_scope (self or pair)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'payment_scope'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN payment_scope TEXT NOT NULL DEFAULT 'self';
        
        -- Add check constraint
        ALTER TABLE entries
        ADD CONSTRAINT check_payment_scope 
        CHECK (payment_scope IN ('self', 'pair'));
    END IF;
    
    -- paid_amount_rub should already exist from migration 003, but check anyway
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'entries' AND column_name = 'paid_amount_rub'
    ) THEN
        ALTER TABLE entries 
        ADD COLUMN paid_amount_rub NUMERIC(10, 2) NULL;
    END IF;
END $$;

-- Update admin_entries_view to include new fields
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








