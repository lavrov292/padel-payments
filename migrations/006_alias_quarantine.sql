-- Migration: Add name normalization, aliases, and pending entries
-- Enables fuzzy matching for player names during import

DO $$
BEGIN
    -- 1. Add normalized_name to players
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'players' AND column_name = 'normalized_name'
    ) THEN
        ALTER TABLE players 
        ADD COLUMN normalized_name TEXT;
        
        -- Create index
        CREATE INDEX IF NOT EXISTS idx_players_normalized_name 
        ON players(normalized_name);
    END IF;
    
    -- 2. Create player_aliases table
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'player_aliases'
    ) THEN
        CREATE TABLE player_aliases (
            id BIGSERIAL PRIMARY KEY,
            alias_name TEXT NOT NULL,
            normalized_alias TEXT NOT NULL UNIQUE,
            player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
            created_by_telegram_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE INDEX idx_player_aliases_player_id ON player_aliases(player_id);
        CREATE INDEX idx_player_aliases_normalized_alias ON player_aliases(normalized_alias);
    END IF;
    
    -- 3. Create pending_entries table
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'pending_entries'
    ) THEN
        CREATE TABLE pending_entries (
            id BIGSERIAL PRIMARY KEY,
            sync_run_id BIGINT REFERENCES sync_runs(id) ON DELETE CASCADE,
            tournament_id BIGINT REFERENCES tournaments(id) ON DELETE CASCADE,
            raw_player_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            payload JSONB NOT NULL,
            candidates JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'expired')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            admin_message_id BIGINT
        );
        
        CREATE INDEX idx_pending_entries_sync_status ON pending_entries(sync_run_id, status);
        CREATE INDEX idx_pending_entries_tournament ON pending_entries(tournament_id);
        CREATE INDEX idx_pending_entries_normalized ON pending_entries(normalized_name);
    END IF;
    
    -- 4. Add data column to telegram_sessions if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'telegram_sessions' AND column_name = 'data'
    ) THEN
        ALTER TABLE telegram_sessions 
        ADD COLUMN data JSONB;
    END IF;
    
END $$;

-- 4. Create PostgreSQL function for name normalization (outside DO block)
-- This matches Python normalize_name() function
CREATE OR REPLACE FUNCTION normalize_name(input_text TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN regexp_replace(
        regexp_replace(
            lower(trim(input_text)),
            'ё', 'е', 'g'
        ),
        '\s+', ' ', 'g'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Now populate normalized_name for existing players (if function was just created)
UPDATE players 
SET normalized_name = normalize_name(full_name)
WHERE normalized_name IS NULL;

