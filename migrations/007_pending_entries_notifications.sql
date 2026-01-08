-- Migration: Add notification and resolution fields to pending_entries
-- Enables admin notifications for pending player name resolutions

DO $$
BEGIN
    -- Add notified_at field
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pending_entries' AND column_name = 'notified_at'
    ) THEN
        ALTER TABLE pending_entries 
        ADD COLUMN notified_at TIMESTAMPTZ;
    END IF;
    
    -- Add resolved_at field
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pending_entries' AND column_name = 'resolved_at'
    ) THEN
        ALTER TABLE pending_entries 
        ADD COLUMN resolved_at TIMESTAMPTZ;
    END IF;
    
    -- Add resolved_player_id field
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'pending_entries' AND column_name = 'resolved_player_id'
    ) THEN
        ALTER TABLE pending_entries 
        ADD COLUMN resolved_player_id BIGINT REFERENCES players(id) ON DELETE SET NULL;
    END IF;
    
    -- Update status check constraint to include 'resolved' and 'snoozed'
    -- Drop old constraint if exists
    IF EXISTS (
        SELECT 1 FROM information_schema.constraint_column_usage 
        WHERE table_name = 'pending_entries' 
        AND constraint_name LIKE '%status%check%'
    ) THEN
        ALTER TABLE pending_entries 
        DROP CONSTRAINT IF EXISTS pending_entries_status_check;
    END IF;
    
    -- Add new constraint with all statuses
    ALTER TABLE pending_entries 
    ADD CONSTRAINT pending_entries_status_check 
    CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'resolved', 'snoozed'));
    
    -- Create index for notified_at queries
    CREATE INDEX IF NOT EXISTS idx_pending_entries_notified 
    ON pending_entries(notified_at) WHERE notified_at IS NULL;
    
END $$;




