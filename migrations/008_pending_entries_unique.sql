-- Migration: Add unique constraint to pending_entries to prevent duplicates
-- Ensures only one pending entry per tournament + normalized_name + status='pending'

DO $$
BEGIN
    -- Drop existing index if it exists (to recreate with correct name)
    DROP INDEX IF EXISTS pending_entries_tournament_normalized_status_unique;
    
    -- Create unique partial index for pending entries
    -- This ensures only one pending entry per (tournament_id, normalized_name) when status='pending'
    CREATE UNIQUE INDEX IF NOT EXISTS pending_entries_tournament_normalized_status_unique 
    ON pending_entries(tournament_id, normalized_name) 
    WHERE status = 'pending';
END $$;

